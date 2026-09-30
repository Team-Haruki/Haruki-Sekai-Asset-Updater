use std::collections::{BTreeMap, HashMap};
use std::sync::{atomic::AtomicBool, Arc, OnceLock};
use std::time::Duration;

use futures_util::TryStreamExt;
use opendal::Operator;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use super::asset_index::{
    check_cancelled, digest, read_bounded, AssetBlob, AssetIndexError, AssetManifest,
};

const MAX_CHARTS: usize = 131_072;
const MAX_INDEX_BYTES: usize = 32 << 20;
const MAX_CHART_BYTES: usize = 64 << 20;
const CHART_CONCURRENCY: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BpmEvent {
    bar: f64,
    bpm: f64,
    duration: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct BpmChart {
    main_bpm: f64,
    events: Vec<BpmEvent>,
    bar_count: usize,
    duration: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BpmIndex {
    schema_version: u32,
    region: String,
    resource_revision: String,
    complete: bool,
    prefixes: Vec<String>,
    charts: BTreeMap<String, BpmChart>,
}

/// Called between the complete asset inventory and its atomic publication.
/// Any failed read/parse aborts publication instead of marking absent charts.
pub(crate) async fn publish_bpm_index(
    operator: &Operator,
    manifest: &mut AssetManifest,
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<(), AssetIndexError> {
    check_cancelled(cancel_flag)?;
    if manifest
        .bpm
        .as_ref()
        .is_some_and(|blob| blob.revision.as_deref() == Some(manifest.revision.as_str()))
    {
        // prepare() only carries a same-revision blob after validating it.
        return Ok(());
    }
    if !manifest.complete
        || manifest.revision.is_empty()
        || !matches!(manifest.region.as_str(), "jp" | "en" | "tw" | "kr" | "cn")
    {
        return Err(invalid("BPM index requires a complete regional inventory"));
    }
    let prefixes: Vec<String> = ["startapp", "ondemand"]
        .iter()
        .map(|mode| format!("{}-assets/{mode}/music/music_score/", manifest.region))
        .collect();
    let keys = list_chart_keys(operator, &prefixes, cancel_flag).await?;
    let charts = read_charts(operator, keys, cancel_flag).await?;
    let index = BpmIndex {
        schema_version: 1,
        region: manifest.region.clone(),
        resource_revision: manifest.revision.clone(),
        complete: true,
        prefixes,
        charts,
    };
    let bytes = sonic_rs::to_vec(&index)?;
    if bytes.len() > MAX_INDEX_BYTES {
        return Err(invalid("BPM index exceeds 32 MiB"));
    }
    let sha256 = digest(&bytes);
    let key = format!("indexes/bpm/v1/{}/{sha256}.json", manifest.region);
    check_cancelled(cancel_flag)?;
    match read_bounded(operator, &key, MAX_INDEX_BYTES as u64).await {
        Ok(existing) if existing == bytes => {}
        Ok(_) => return Err(invalid("immutable BPM index contains different bytes")),
        Err(AssetIndexError::Storage(err)) if err.kind() == opendal::ErrorKind::NotFound => {
            operator
                .write_with(&key, bytes)
                .content_type("application/json")
                .cache_control("public,max-age=31536000,immutable")
                .await?;
        }
        Err(err) => return Err(err),
    }
    check_cancelled(cancel_flag)?;
    manifest.bpm = Some(AssetBlob {
        key,
        sha256,
        revision: Some(manifest.revision.clone()),
    });
    Ok(())
}

async fn list_chart_keys(
    operator: &Operator,
    prefixes: &[String],
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<Vec<String>, AssetIndexError> {
    let mut keys = Vec::new();
    for prefix in prefixes {
        check_cancelled(cancel_flag)?;
        let mut lister = operator.lister_with(prefix).recursive(true).await?;
        while let Some(entry) = lister.try_next().await? {
            check_cancelled(cancel_flag)?;
            if entry.metadata().mode().is_file() && is_chart_key(prefix, entry.path()) {
                if keys.len() >= MAX_CHARTS {
                    return Err(invalid("BPM index exceeds chart count limit"));
                }
                keys.push(entry.path().to_owned());
            }
        }
    }
    keys.sort();
    keys.dedup();
    Ok(keys)
}

fn is_chart_key(prefix: &str, key: &str) -> bool {
    let Some(rest) = key.strip_prefix(prefix) else {
        return false;
    };
    let Some((directory, filename)) = rest.split_once('/') else {
        return false;
    };
    let Some(id) = directory.strip_suffix("_01") else {
        return false;
    };
    if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    let filename = filename.to_ascii_lowercase();
    matches!(
        filename.as_str(),
        "easy.txt" | "normal.txt" | "hard.txt" | "expert.txt" | "master.txt" | "append.txt"
    )
}

async fn read_charts(
    operator: &Operator,
    keys: Vec<String>,
    cancel_flag: &Option<Arc<AtomicBool>>,
) -> Result<BTreeMap<String, BpmChart>, AssetIndexError> {
    let mut charts = BTreeMap::new();
    let mut tasks = JoinSet::new();
    for key in keys {
        check_cancelled(cancel_flag)?;
        if tasks.len() >= CHART_CONCURRENCY {
            collect_chart(&mut tasks, &mut charts).await?;
        }
        let operator = operator.clone();
        let cancel_flag = cancel_flag.clone();
        tasks.spawn(async move {
            check_cancelled(&cancel_flag)?;
            let result = tokio::time::timeout(Duration::from_secs(30), async {
                let bytes = read_bounded(&operator, &key, MAX_CHART_BYTES as u64).await?;
                check_cancelled(&cancel_flag)?;
                parse_chart_bpm(&bytes)
            })
            .await;
            let chart = match result {
                Ok(result) => result,
                Err(_) => Err(invalid("BPM chart read timed out")),
            }
            .map_err(|err| err.at_object(&key))?;
            Ok((key, chart))
        });
    }
    while !tasks.is_empty() {
        check_cancelled(cancel_flag)?;
        collect_chart(&mut tasks, &mut charts).await?;
    }
    Ok(charts)
}

async fn collect_chart(
    tasks: &mut JoinSet<Result<(String, BpmChart), AssetIndexError>>,
    charts: &mut BTreeMap<String, BpmChart>,
) -> Result<(), AssetIndexError> {
    if let Some(result) = tasks.join_next().await {
        let (key, chart) = result.map_err(|_| invalid("BPM chart task failed"))??;
        charts.insert(key, chart);
    }
    Ok(())
}

// Keep this parser aligned with Cloud music/lookup_cover_bpm.go. In particular,
// duplicate SUS tokens overwrite, non-BPM channels still extend bar_count,
// and equal-duration dominant BPMs prefer the first occurrence.
fn parse_chart_bpm(bytes: &[u8]) -> Result<BpmChart, AssetIndexError> {
    static LINE_PATTERN: OnceLock<Regex> = OnceLock::new();
    let pattern = LINE_PATTERN.get_or_init(|| {
        Regex::new(r"^#([A-Za-z0-9]{3})([A-Za-z0-9]{2})[ \t\n\f\r]*:[ \t\n\f\r]*([^ \t\n\f\r]+)")
            .expect("constant SUS token pattern")
    });
    let text = std::str::from_utf8(bytes).map_err(|_| invalid("chart is not UTF-8"))?;
    let mut score = BTreeMap::new();
    let mut bar_count = 0;
    for line in text.lines() {
        if line.len() >= 65_536 {
            return Err(invalid("chart line exceeds SUS scanner limit"));
        }
        let Some(captures) = pattern.captures(line.trim()) else {
            continue;
        };
        let bar = captures[1].to_ascii_uppercase();
        let channel = captures[2].to_ascii_uppercase();
        if let Ok(number) = bar.parse::<usize>() {
            bar_count = bar_count.max(number + 1);
        }
        score.insert((bar, channel), captures[3].trim().to_owned());
    }
    let mut palette = BTreeMap::new();
    for ((bar, channel), value) in &score {
        if bar == "BPM" {
            if let Ok(bpm) = value.parse::<f64>() {
                palette.insert(channel.clone(), bpm);
            }
        }
    }
    let mut raw_events = Vec::new();
    for ((bar, channel), value) in &score {
        if channel != "08" {
            continue;
        }
        let Ok(bar) = bar.parse::<usize>() else {
            continue;
        };
        let count = value.len() / 2;
        if count == 0 {
            continue;
        }
        for (position, token) in value.as_bytes().chunks_exact(2).enumerate() {
            let Ok(token) = std::str::from_utf8(token) else {
                continue;
            };
            let token = token.to_ascii_uppercase();
            if token == "00" {
                continue;
            }
            if let Some(&bpm) = palette.get(&token) {
                if !bpm.is_finite() || bpm <= 0.0 {
                    return Err(invalid("invalid chart BPM"));
                }
                raw_events.push(BpmEvent {
                    bar: bar as f64 + position as f64 / count as f64,
                    bpm,
                    duration: 0.0,
                });
            }
        }
    }
    raw_events.sort_by(|a, b| a.bar.total_cmp(&b.bar));
    let mut events: Vec<BpmEvent> = Vec::new();
    for event in raw_events {
        if events
            .last()
            .is_some_and(|previous| previous.bpm == event.bpm)
        {
            continue;
        }
        events.push(event);
    }
    if events.is_empty() {
        return Err(invalid("chart contains no BPM events"));
    }
    let mut duration = 0.0;
    let mut durations = HashMap::<u64, f64>::new();
    for index in 0..events.len() {
        let next = events
            .get(index + 1)
            .map_or(bar_count as f64, |event| event.bar);
        let event = &mut events[index];
        event.duration = (next - event.bar) / event.bpm * 4.0 * 60.0;
        if !event.duration.is_finite() || event.duration < 0.0 {
            return Err(invalid("invalid chart duration"));
        }
        duration += event.duration;
        if !duration.is_finite() {
            return Err(invalid("invalid total chart duration"));
        }
        *durations.entry(event.bpm.to_bits()).or_default() += event.duration;
    }
    let mut main_bpm = 0.0;
    let mut main_duration = -1.0;
    for event in &events {
        let duration = durations[&event.bpm.to_bits()];
        if duration > main_duration {
            main_bpm = event.bpm;
            main_duration = duration;
        }
    }
    Ok(BpmChart {
        main_bpm,
        events,
        bar_count,
        duration,
    })
}

fn invalid(message: &str) -> AssetIndexError {
    AssetIndexError::Invalid(message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use opendal::services;
    use std::sync::atomic::Ordering;

    const FIXTURE: &str =
        "  #BPM01:90\n#BPM01:120\n#BPM02:240\n#00008:0100\n#00108:0002\n#00211:11\n";

    fn manifest() -> AssetManifest {
        AssetManifest {
            version: 1,
            region: "jp".into(),
            revision: "r1".into(),
            complete: true,
            published_at: Utc::now(),
            shards: vec![],
            bpm: None,
        }
    }

    fn operator(root: &std::path::Path) -> Operator {
        Operator::new(services::Fs::default().root(root.to_str().unwrap())).unwrap()
    }

    #[test]
    fn cross_language_fixture() {
        let chart = parse_chart_bpm(FIXTURE.as_bytes()).unwrap();
        assert_eq!(chart.main_bpm, 120.0);
        assert_eq!(chart.bar_count, 3);
        assert_eq!(chart.duration, 4.5);
        assert_eq!(
            chart.events,
            vec![
                BpmEvent {
                    bar: 0.0,
                    bpm: 120.0,
                    duration: 3.0
                },
                BpmEvent {
                    bar: 1.5,
                    bpm: 240.0,
                    duration: 1.5
                }
            ]
        );
        println!(
            "BPM_CROSS_LANGUAGE={}",
            sonic_rs::to_string(&chart).unwrap()
        );
    }

    #[test]
    fn parser_rejects_invalid_charts_and_ties_are_stable() {
        for chart in [
            b"".as_slice(),
            b"no BPM",
            b"#BPM01:0\n#00008:01",
            b"#BPM01:NaN\n#00008:01",
            b"#BPM01:inf\n#00008:01",
            b"\xff",
        ] {
            assert!(parse_chart_bpm(chart).is_err());
        }
        assert!(parse_chart_bpm("x".repeat(65_536).as_bytes()).is_err());
        let chart = parse_chart_bpm(b"#BPM01:120\n#BPM02:240\n#00008:0100\n#00108:0200\n#00211:11")
            .unwrap();
        assert_eq!(chart.main_bpm, 120.0);
        let chart = parse_chart_bpm(b"#BPM01:120\n#00008:0101\n#00108:0001").unwrap();
        assert_eq!(chart.events.len(), 1);
    }

    #[test]
    fn chart_keys_only_include_candidate_scores() {
        let prefix = "jp-assets/startapp/music/music_score/";
        assert!(is_chart_key(prefix, &format!("{prefix}0001_01/expert.txt")));
        assert!(is_chart_key(prefix, &format!("{prefix}0001_01/EXPERT.TXT")));
        for suffix in [
            "0001_02/expert.txt",
            "x_01/expert.txt",
            "_01/expert.txt",
            "0001_01/readme.txt",
            "0001_01/expert.png",
            "0001_01/dir/expert.txt",
        ] {
            assert!(!is_chart_key(prefix, &format!("{prefix}{suffix}")));
        }
        assert!(!is_chart_key(prefix, "other/chart.txt"));
    }

    #[tokio::test]
    async fn publishes_complete_immutable_index_and_reuses_revision() {
        let root = tempfile::tempdir().unwrap();
        let op = operator(root.path());
        op.write(
            "jp-assets/startapp/music/music_score/0001_01/expert.txt",
            FIXTURE.as_bytes().to_vec(),
        )
        .await
        .unwrap();
        op.write(
            "jp-assets/ondemand/music/music_score/0001_01/master.txt",
            FIXTURE.as_bytes().to_vec(),
        )
        .await
        .unwrap();
        op.write(
            "jp-assets/ondemand/music/music_score/0001_02/expert.txt",
            b"not used".to_vec(),
        )
        .await
        .unwrap();
        let mut manifest = manifest();
        publish_bpm_index(&op, &mut manifest, &None).await.unwrap();
        let reference = manifest.bpm.clone().unwrap();
        let bytes = op.read(&reference.key).await.unwrap().to_vec();
        assert_eq!(digest(&bytes), reference.sha256);
        let index: BpmIndex = sonic_rs::from_slice(&bytes).unwrap();
        assert_eq!(index.charts.len(), 2);
        assert!(index.complete);
        assert_eq!(index.prefixes.len(), 2);
        publish_bpm_index(&op, &mut manifest, &None).await.unwrap();
        assert_eq!(manifest.bpm.as_ref().unwrap().key, reference.key);
        manifest.bpm = None;
        publish_bpm_index(&op, &mut manifest, &None).await.unwrap();
        assert_eq!(manifest.bpm.as_ref().unwrap().key, reference.key);
    }

    #[tokio::test]
    async fn malformed_chart_or_cancellation_does_not_publish_reference() {
        let root = tempfile::tempdir().unwrap();
        let op = operator(root.path());
        op.write(
            "jp-assets/startapp/music/music_score/0001_01/expert.txt",
            b"invalid".to_vec(),
        )
        .await
        .unwrap();
        let mut manifest = manifest();
        let error = publish_bpm_index(&op, &mut manifest, &None)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("jp-assets/startapp/music/music_score/0001_01/expert.txt"));
        assert!(manifest.bpm.is_none());
        let flag = Arc::new(AtomicBool::new(true));
        assert!(matches!(
            publish_bpm_index(&op, &mut manifest, &Some(flag.clone())).await,
            Err(AssetIndexError::Cancelled)
        ));
        assert!(manifest.bpm.is_none());
        flag.store(false, Ordering::Relaxed);
        manifest.complete = false;
        assert!(publish_bpm_index(&op, &mut manifest, &Some(flag))
            .await
            .is_err());
    }
}
