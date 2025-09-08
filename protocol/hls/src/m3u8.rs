use chrono::prelude::*;
use chrono::Duration;
use {
    super::{errors::MediaError, ts::Ts},
    bytes::BytesMut,
    std::{collections::VecDeque, fs, fs::File, io::Write, path::PathBuf},
};

/**
Representation of a M3u8 HLS Segment
*/
pub struct Segment {
    pub duration: i64, // ts fragment duration in ms
    pub discontinuity: bool,
    pub name: String, // ts fragment name
    path: PathBuf,
    pub is_eof: bool,
    pub pdt: Option<DateTime<Utc>>, // Program Data Time (time when it was broadcasted)
}

impl Segment {
    pub fn new(
        duration: i64,
        discontinuity: bool,
        name: String,
        path: PathBuf,
        is_eof: bool,
        pdt: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            duration,
            discontinuity,
            name,
            path,
            is_eof,
            pdt,
        }
    }
}

pub struct M3u8 {
    version: u16,
    sequence_no: u64,
    /*What duration should media files be?
    A duration of 10 seconds of media per file seems to strike a reasonable balance for most broadcast content.
    http://devimages.apple.com/iphone/samples/bipbop/bipbopall.m3u8*/
    duration: i64,
    /*How many files should be listed in the index file during a continuous, ongoing session?
    The normal recommendation is 3, but the optimum number may be larger.*/
    live_ts_count: usize,

    segments: VecDeque<Segment>,

    m3u8_folder: String,
    live_m3u8_name: String,

    ts_handler: Ts,

    need_record: bool,
    vod_m3u8_content: String,
    vod_m3u8_name: String,
}

impl M3u8 {
    pub fn new(
        duration: i64,
        live_ts_count: usize,
        app_name: String,
        stream_name: String,
        need_record: bool,
        data_dir: Option<String>,
    ) -> Self {
        let base = data_dir.unwrap_or_else(|| String::from("."));
        let app_clone = app_name.clone();
        let stream_clone = stream_name.clone();
        let m3u8_folder = format!("{}/{}/{}", base, app_name, stream_name);
        fs::create_dir_all(m3u8_folder.clone()).unwrap();

        // Use a fixed name for the live playlist so that it is always served
        // as `/live/<stream>/index.m3u8` regardless of the stream name.
        let live_m3u8_name = String::from("index.m3u8");
        let vod_m3u8_name = if need_record {
            format!("vod_{stream_clone}.m3u8")
        } else {
            String::default()
        };

        let mut m3u8 = Self {
            version: 3,
            sequence_no: 0,
            duration,
            live_ts_count,
            segments: VecDeque::new(),
            m3u8_folder,
            live_m3u8_name,
            ts_handler: Ts::new(app_clone, stream_clone, Some(base)),
            // record,
            need_record,
            vod_m3u8_content: String::default(),
            vod_m3u8_name,
        };

        if need_record {
            m3u8.vod_m3u8_content = m3u8.generate_m3u8_header(true);
        }
        m3u8
    }

    pub fn add_segment(
        &mut self,
        duration: i64,
        discontinuity: bool,
        is_eof: bool,
        ts_data: BytesMut,
    ) -> Result<(), MediaError> {
        let segment_count = self.segments.len();

        if segment_count >= self.live_ts_count {
            let segment = self.segments.pop_front().unwrap();
            if !self.need_record {
                if let Err(err) = self.ts_handler.delete(&segment.path) {
                    log::error!(
                        "failed to delete segment file {}: {}",
                        segment.path.display(),
                        err
                    );
                }
            }

            self.sequence_no += 1;
        }
        self.duration = std::cmp::max(duration, self.duration);

        // Determine date/time for the new segment:
        // If there is already a segment with PDT -> integrate forward,
        // otherwise assume “now” as the start time.
        let next_pdt = if let Some(prev) = self.segments.back() {
            prev.pdt.map(|t| t + Duration::milliseconds(prev.duration))
        } else {
            Some(Utc::now())
        };

        let (ts_name, ts_path) = self.ts_handler.write(ts_data, next_pdt)?;
        let segment = Segment::new(duration, discontinuity, ts_name, ts_path, is_eof, next_pdt);

        if self.need_record {
            self.update_vod_m3u8(&segment);
        }

        self.segments.push_back(segment);

        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), MediaError> {
        if self.need_record {
            let vod_m3u8_path = format!("{}/{}", self.m3u8_folder, self.vod_m3u8_name);
            let mut file_handler = File::create(vod_m3u8_path)?;
            self.vod_m3u8_content += "#EXT-X-ENDLIST\n";
            file_handler.write_all(self.vod_m3u8_content.as_bytes())?;
        } else {
            for segment in &self.segments {
                if let Err(err) = self.ts_handler.delete(&segment.path) {
                    log::error!(
                        "failed to delete segment file during clear {}: {}",
                        segment.path.display(),
                        err
                    );
                }
            }
        }

        //clear live m3u8
        let live_m3u8_path = format!("{}/{}", self.m3u8_folder, self.live_m3u8_name);
        fs::remove_file(live_m3u8_path)?;

        Ok(())
    }

    pub fn generate_m3u8_header(&self, is_vod: bool) -> String {
        let mut m3u8_header = "#EXTM3U\n".to_string();
        m3u8_header += format!("#EXT-X-VERSION:{}\n", self.version).as_str();
        m3u8_header += format!("#EXT-X-TARGETDURATION:{}\n", (self.duration + 999) / 1000).as_str();

        if is_vod {
            m3u8_header += "#EXT-X-MEDIA-SEQUENCE:0\n";
            m3u8_header += "#EXT-X-PLAYLIST-TYPE:VOD\n";
            if self.version <= 7 {
                // allow cache is deprecated/removed in HLS Version 7 and up
                m3u8_header += "#EXT-X-ALLOW-CACHE:YES\n";
            }
        } else {
            m3u8_header += format!("#EXT-X-MEDIA-SEQUENCE:{}\n", self.sequence_no).as_str();
            m3u8_header += "#EXT-X-PLAYLIST-TYPE:EVENT\n";
        }

        m3u8_header
    }

    pub fn refresh_playlist(&mut self) -> Result<String, MediaError> {
        let mut m3u8_content = self.generate_m3u8_header(false);

        for segment in &self.segments {
            if segment.discontinuity {
                m3u8_content += "#EXT-X-DISCONTINUITY\n";
            }
            if let Some(pdt) = segment.pdt {
                m3u8_content += &format!(
                    "#EXT-X-PROGRAM-DATE-TIME:{}\n",
                    pdt.to_rfc3339_opts(SecondsFormat::Millis, false)
                );
            }
            m3u8_content += &format!(
                "#EXTINF:{:.3}\n{}\n",
                segment.duration as f64 / 1000.0,
                segment.name
            );

            if segment.is_eof {
                m3u8_content += "#EXT-X-ENDLIST\n";
                break;
            }
        }

        let m3u8_path = format!("{}/{}", self.m3u8_folder, self.live_m3u8_name);

        let mut file_handler = File::create(m3u8_path)?;
        file_handler.write_all(m3u8_content.as_bytes())?;

        Ok(m3u8_content)
    }

    pub fn update_vod_m3u8(&mut self, segment: &Segment) {
        if segment.discontinuity {
            self.vod_m3u8_content += "#EXT-X-DISCONTINUITY\n";
        }
        self.vod_m3u8_content += format!(
            "#EXTINF:{:.3}\n{}\n",
            segment.duration as f64 / 1000.0,
            segment.name
        )
        .as_str();
    }
}
