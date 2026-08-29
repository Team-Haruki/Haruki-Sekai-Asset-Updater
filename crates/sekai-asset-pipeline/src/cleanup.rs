use std::io;
use std::path::Path;
use std::thread;
use std::time::Duration;

const REMOVE_FILE_ATTEMPTS: usize = 8;
const REMOVE_FILE_RETRY_DELAY: Duration = Duration::from_millis(25);

pub fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match remove_file_with_retries(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

pub fn remove_file_with_retries(path: &Path) -> io::Result<()> {
    let mut last_error = None;
    for attempt in 0..REMOVE_FILE_ATTEMPTS {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) if is_retryable_remove_error(&err) && attempt + 1 < REMOVE_FILE_ATTEMPTS => {
                last_error = Some(err);
                thread::sleep(REMOVE_FILE_RETRY_DELAY);
            }
            Err(err) => return Err(err),
        }
    }
    Err(last_error
        .unwrap_or_else(|| io::Error::other(format!("failed to remove {}", path.display()))))
}

fn is_retryable_remove_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::Other
    )
}
