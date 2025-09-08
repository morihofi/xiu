use {
    super::errors::MediaError,
    bytes::BytesMut,
    chrono::prelude::*,
    std::{
        fs,
        fs::File,
        io::Write,
        path::{Path, PathBuf},
    },
};

pub struct Ts {
    live_path: PathBuf,
}

impl Ts {
    pub fn new(app_name: String, stream_name: String, data_dir: Option<String>) -> Self {
        let base = data_dir.unwrap_or_else(|| String::from("."));
        let live_path = PathBuf::from(base).join(app_name).join(stream_name);
        fs::create_dir_all(&live_path).unwrap();

        Self { live_path }
    }
    pub fn write(
        &mut self,
        data: BytesMut,
        pdt: Option<DateTime<Utc>>,
    ) -> Result<(String, PathBuf), MediaError> {
        let ts_time = pdt.unwrap_or_else(Utc::now);
        let ts_file_name = format!("{}.ts", ts_time.format("%Y%m%dT%H%M%S"));
        let ts_file_path = self.live_path.join(&ts_file_name);

        let mut ts_file_handler = File::create(&ts_file_path)?;
        ts_file_handler.write_all(&data[..])?;

        Ok((ts_file_name, ts_file_path))
    }
    pub fn delete<P: AsRef<Path>>(&mut self, ts_file_path: P) -> Result<(), MediaError> {
        fs::remove_file(ts_file_path.as_ref())?;
        Ok(())
    }
}
