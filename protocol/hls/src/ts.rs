use {
    super::errors::MediaError,
    bytes::BytesMut,
    chrono::prelude::*,
    std::{fs, fs::File, io::Write},
};

pub struct Ts {
    live_path: String,
}

impl Ts {
    pub fn new(app_name: String, stream_name: String, data_dir: Option<String>) -> Self {
        let base = data_dir.unwrap_or_else(|| String::from("."));
        let live_path = format!("{}/{}/{}", base, app_name, stream_name);
        fs::create_dir_all(live_path.clone()).unwrap();

        Self { live_path }
    }
    pub fn write(
        &mut self,
        data: BytesMut,
        pdt: Option<DateTime<Utc>>,
    ) -> Result<(String, String), MediaError> {
        let ts_time = pdt.unwrap_or_else(Utc::now);
        let ts_file_name = format!("{}.ts", ts_time.format("%Y%m%dT%H%M%S"));
        let ts_file_path = format!("{}/{}", self.live_path, ts_file_name);

        let mut ts_file_handler = File::create(&ts_file_path)?;
        ts_file_handler.write_all(&data[..])?;

        Ok((ts_file_name, ts_file_path))
    }
    pub fn delete(&mut self, ts_file_name: String) {
        fs::remove_file(ts_file_name).unwrap();
    }
}
