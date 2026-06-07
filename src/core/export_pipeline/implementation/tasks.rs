use super::*;

pub(super) fn asset_studio_export_type_list(region: &RegionConfig) -> Vec<String> {
    let mut export_types = Vec::new();
    for asset_type in &region.export.asset_studio_types {
        let asset_type = asset_type.trim();
        let asset_type = assetstudio_export_type_selector(asset_type).unwrap_or(asset_type);
        if asset_type.is_empty() || export_types.iter().any(|value| value == asset_type) {
            continue;
        }
        export_types.push(asset_type.to_string());
    }

    if export_types.is_empty() {
        DEFAULT_ASSET_STUDIO_EXPORT_TYPES
            .iter()
            .map(|value| (*value).to_string())
            .collect()
    } else {
        export_types
    }
}

pub(super) fn run_path_tasks<F>(
    paths: Vec<PathBuf>,
    concurrency: usize,
    task: F,
) -> Result<Vec<PathBuf>, ExportPipelineError>
where
    F: Fn(PathBuf) -> Result<Vec<PathBuf>, ExportPipelineError> + Send + Sync + 'static,
{
    let results = run_tasks(paths, concurrency, task)?;
    Ok(results.into_iter().flatten().collect())
}

pub(super) fn run_tasks<I, T, F>(
    paths: Vec<I>,
    concurrency: usize,
    task: F,
) -> Result<Vec<T>, ExportPipelineError>
where
    I: Send + 'static,
    T: Send + 'static,
    F: Fn(I) -> Result<T, ExportPipelineError> + Send + Sync + 'static,
{
    if paths.is_empty() {
        return Ok(Vec::new());
    }
    if paths.len() == 1 {
        return paths.into_iter().map(task).collect();
    }

    let worker_count = concurrency.max(1).min(paths.len());
    let queue = Arc::new(Mutex::new(VecDeque::from(paths)));
    let results = Arc::new(Mutex::new(Vec::<T>::new()));
    let first_error = Arc::new(Mutex::new(None::<ExportPipelineError>));
    let task = Arc::new(task);
    let mut handles = Vec::with_capacity(worker_count);
    const WORKER_STACK_SIZE: usize = 32 * 1024 * 1024;

    for _ in 0..worker_count {
        let queue = queue.clone();
        let results = results.clone();
        let first_error = first_error.clone();
        let task = task.clone();
        let worker_name = "export-task".to_string();
        let handle = std::thread::Builder::new()
            .name(worker_name.clone())
            .stack_size(WORKER_STACK_SIZE)
            .spawn(move || loop {
                if first_error.lock().unwrap().is_some() {
                    break;
                }

                let next_path = queue.lock().unwrap().pop_front();
                let Some(path) = next_path else {
                    break;
                };

                match task(path) {
                    Ok(generated) => results.lock().unwrap().push(generated),
                    Err(err) => {
                        let mut first = first_error.lock().unwrap();
                        if first.is_none() {
                            *first = Some(err);
                        }
                        break;
                    }
                }
            })
            .map_err(|source| ExportPipelineError::WorkerSpawn {
                worker: worker_name,
                source,
            })?;
        handles.push(handle);
    }

    for handle in handles {
        handle
            .join()
            .map_err(|panic| ExportPipelineError::WorkerPanic {
                worker: "export task".to_string(),
                message: panic_message(panic),
            })?;
    }

    if let Some(err) = first_error.lock().unwrap().take() {
        return Err(err);
    }

    let mut results = results.lock().unwrap();
    Ok(std::mem::take(&mut *results))
}

pub(super) fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown worker panic".to_string()
    }
}

#[derive(Debug, Default)]
pub(super) struct UsmProcessingInputs {
    pub(super) files: Vec<UsmProcessingInput>,
    pub(super) merged_count: usize,
}

type UsmSegmentGroupKey = Option<(PathBuf, String)>;
type UsmSegmentGroupEntry = (usize, usize, PathBuf);

#[derive(Debug, PartialEq, Eq)]
pub(super) enum UsmProcessingInput {
    Path(PathBuf),
    Bytes {
        output_dir: PathBuf,
        output_name: String,
        fallback_name: String,
        data: Vec<u8>,
        source_files: Vec<PathBuf>,
    },
}

impl UsmProcessingInput {
    pub(super) fn path(&self) -> Option<&Path> {
        match self {
            Self::Path(path) => Some(path),
            Self::Bytes { .. } => None,
        }
    }

    pub(super) fn output_dir(&self) -> PathBuf {
        match self {
            Self::Path(path) => path
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
            Self::Bytes { output_dir, .. } => output_dir.clone(),
        }
    }

    pub(super) fn output_name(&self) -> Result<String, ExportPipelineError> {
        match self {
            Self::Path(path) => path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
                .ok_or_else(|| ExportPipelineError::Io {
                    path: path.clone(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid usm file name",
                    ),
                }),
            Self::Bytes { output_name, .. } => Ok(output_name.clone()),
        }
    }

    pub(super) fn cleanup_sources(&self) -> Result<(), ExportPipelineError> {
        match self {
            Self::Path(path) => remove_export_file_if_exists(path),
            Self::Bytes { source_files, .. } => {
                for source_file in source_files {
                    remove_export_file_if_exists(source_file)?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn output_sort_key(&self) -> PathBuf {
        match self {
            Self::Path(path) => path.clone(),
            Self::Bytes {
                output_dir,
                output_name,
                ..
            } => output_dir.join(format!("{output_name}.usm")),
        }
    }
}

pub(super) fn prepare_usm_processing_inputs(
    usm_files: Vec<PathBuf>,
) -> Result<UsmProcessingInputs, ExportPipelineError> {
    if usm_files.len() <= 1 {
        return Ok(UsmProcessingInputs {
            files: usm_files
                .into_iter()
                .map(UsmProcessingInput::Path)
                .collect(),
            merged_count: 0,
        });
    }

    let mut indexed = Vec::with_capacity(usm_files.len());
    for (index, path) in usm_files.iter().enumerate() {
        indexed.push((usm_segment_key(path), index, path.clone()));
    }

    let mut groups: BTreeMap<UsmSegmentGroupKey, Vec<UsmSegmentGroupEntry>> = BTreeMap::new();
    for (key, index, path) in indexed {
        if let Some((dir, stem, segment)) = key {
            groups
                .entry(Some((dir, stem)))
                .or_default()
                .push((segment, index, path));
        } else {
            groups
                .entry(None)
                .or_default()
                .push((usize::MAX, index, path));
        }
    }

    let mut prepared = Vec::new();
    let mut merged_count = 0;
    for (key, mut group) in groups {
        group.sort_by_key(|(segment, index, _)| (*segment, *index));
        if let Some((dir, stem)) = key {
            let is_contiguous_segmented_usm = group.len() > 1
                && group
                    .iter()
                    .enumerate()
                    .all(|(index, (segment, _, _))| *segment == index + 1);
            if is_contiguous_segmented_usm {
                let sources = group
                    .into_iter()
                    .map(|(_, _, path)| path)
                    .collect::<Vec<_>>();
                let merged = read_usm_segment_files_to_memory(&dir, &stem, &sources)?;
                merged_count += sources.len();
                prepared.push(merged);
                continue;
            }
        }

        prepared.extend(
            group
                .into_iter()
                .map(|(_, _, path)| UsmProcessingInput::Path(path)),
        );
    }

    prepared.sort_by_key(UsmProcessingInput::output_sort_key);
    Ok(UsmProcessingInputs {
        files: prepared,
        merged_count,
    })
}

pub(super) fn usm_segment_key(path: &Path) -> Option<(PathBuf, String, usize)> {
    let stem = path.file_stem()?.to_str()?;
    let (prefix, raw_segment) = stem.rsplit_once('-')?;
    let segment = raw_segment
        .rsplit_once("__dup")
        .and_then(|(segment, duplicate)| {
            (!duplicate.is_empty() && duplicate.bytes().all(|byte| byte.is_ascii_digit()))
                .then_some(segment)
        })
        .unwrap_or(raw_segment);
    if prefix.is_empty() || segment.is_empty() || !segment.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let segment = segment.parse::<usize>().ok()?;
    if segment == 0 {
        return None;
    }
    Some((
        path.parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from(".")),
        prefix.to_string(),
        segment,
    ))
}

pub(super) fn read_usm_segment_files_to_memory(
    dir: &Path,
    stem: &str,
    usm_files: &[PathBuf],
) -> Result<UsmProcessingInput, ExportPipelineError> {
    let mut data = Vec::new();
    for source_path in usm_files {
        let mut source =
            std::fs::File::open(source_path).map_err(|source| ExportPipelineError::Io {
                path: source_path.clone(),
                source,
            })?;
        std::io::copy(&mut source, &mut data).map_err(|source| ExportPipelineError::Io {
            path: source_path.clone(),
            source,
        })?;
    }

    Ok(UsmProcessingInput::Bytes {
        output_dir: dir.to_path_buf(),
        output_name: stem.to_string(),
        fallback_name: format!("{stem}.usm"),
        data,
        source_files: usm_files.to_vec(),
    })
}

pub(super) fn merge_usm_inputs(
    dir: &Path,
    usm_inputs: Vec<UsmProcessingInput>,
) -> Result<PathBuf, ExportPipelineError> {
    let dir_name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("merged");
    let merged_file = dir.join(format!("{dir_name}.usm"));
    let mut target =
        std::fs::File::create(&merged_file).map_err(|source| ExportPipelineError::Io {
            path: merged_file.clone(),
            source,
        })?;

    for input in usm_inputs {
        match input {
            UsmProcessingInput::Path(source_path) => {
                if source_path == merged_file {
                    continue;
                }
                let mut source = std::fs::File::open(&source_path).map_err(|source| {
                    ExportPipelineError::Io {
                        path: source_path.clone(),
                        source,
                    }
                })?;
                std::io::copy(&mut source, &mut target).map_err(|source| {
                    ExportPipelineError::Io {
                        path: source_path.clone(),
                        source,
                    }
                })?;
                remove_export_file_if_exists(&source_path)?;
            }
            UsmProcessingInput::Bytes {
                data, source_files, ..
            } => {
                std::io::copy(&mut data.as_slice(), &mut target).map_err(|source| {
                    ExportPipelineError::Io {
                        path: merged_file.clone(),
                        source,
                    }
                })?;
                for source_file in source_files {
                    remove_export_file_if_exists(&source_file)?;
                }
            }
        }
    }

    Ok(merged_file)
}

pub(super) fn scan_all_files(dir: &Path) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let mut files = Vec::new();
    walk(dir, &mut |path| files.push(path.to_path_buf()))?;
    Ok(files)
}

pub(super) fn remove_export_file_if_exists(path: &Path) -> Result<(), ExportPipelineError> {
    remove_file_if_exists(path).map_err(|source| ExportPipelineError::Io {
        path: path.to_path_buf(),
        source,
    })
}

pub(super) fn find_files_by_extension(
    dir: &Path,
    ext: &str,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    let target_ext = ext.to_lowercase();
    let mut files = Vec::new();
    walk(dir, &mut |path| {
        if path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.eq_ignore_ascii_case(&target_ext))
            .unwrap_or(false)
        {
            files.push(path.to_path_buf());
        }
    })?;
    Ok(files)
}

pub(super) fn post_process_files_by_extension(
    export_path: &Path,
    scoped_post_process: bool,
    scoped_files: &[PathBuf],
    ext: &str,
) -> Result<Vec<PathBuf>, ExportPipelineError> {
    if !scoped_post_process {
        return find_files_by_extension(export_path, ext);
    }

    Ok(scoped_files
        .iter()
        .filter(|path| {
            path.extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(ext))
        })
        .filter(|path| path.exists())
        .cloned()
        .collect())
}

pub(super) fn walk(dir: &Path, f: &mut dyn FnMut(&Path)) -> Result<(), ExportPipelineError> {
    for entry in std::fs::read_dir(dir).map_err(|source| ExportPipelineError::Io {
        path: dir.to_path_buf(),
        source,
    })? {
        let entry = entry.map_err(|source| ExportPipelineError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| ExportPipelineError::Io {
                path: path.clone(),
                source,
            })?;
        if file_type.is_dir() {
            walk(&path, f)?;
        } else {
            f(&path);
        }
    }
    Ok(())
}
