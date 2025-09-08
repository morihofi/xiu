use crate::adapter::RtspAdapter;

streamhub::stream_writer!(RtspWriter, RtspAdapter, "rtsp", channel_id: u8);
