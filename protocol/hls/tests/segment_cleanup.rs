use bytes::BytesMut;
use hls::errors::MediaError;
use hls::m3u8::M3u8;
use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

fn temp_dir() -> PathBuf {
    let mut base = std::env::temp_dir();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    base.push(format!("hls_test_{unique}"));
    base
}

#[test]
fn segment_file_removed_when_evicted() -> Result<(), MediaError> {
    let base = temp_dir();
    let app = "app".to_string();
    let stream = "stream".to_string();

    let mut m3u8 = M3u8::new(
        5,
        1,
        app.clone(),
        stream.clone(),
        false,
        Some(base.to_string_lossy().into_owned()),
    );
    let media_dir = base.join(&app).join(&stream);

    let data = BytesMut::from(&b"dummy"[..]);

    m3u8.add_segment(5, false, false, data.clone())?;
    let first_path = fs::read_dir(&media_dir)?.next().unwrap()?.path();
    assert!(first_path.exists());

    thread::sleep(StdDuration::from_secs(1));
    m3u8.add_segment(5, false, false, data.clone())?;
    assert!(!first_path.exists());

    m3u8.refresh_playlist()?;
    m3u8.clear()?;
    fs::remove_dir_all(base)?;
    Ok(())
}

#[test]
fn segment_files_removed_on_clear() -> Result<(), MediaError> {
    let base = temp_dir();
    let app = "app".to_string();
    let stream = "stream".to_string();

    let mut m3u8 = M3u8::new(
        5,
        5,
        app.clone(),
        stream.clone(),
        false,
        Some(base.to_string_lossy().into_owned()),
    );
    let media_dir = base.join(&app).join(&stream);

    let data = BytesMut::from(&b"dummy"[..]);

    m3u8.add_segment(5, false, false, data.clone())?;
    let first_path = fs::read_dir(&media_dir)?.next().unwrap()?.path();
    assert!(first_path.exists());

    m3u8.refresh_playlist()?;
    m3u8.clear()?;

    assert!(!first_path.exists());
    fs::remove_dir_all(base)?;
    Ok(())
}
