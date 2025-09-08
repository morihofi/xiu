use crate::adapter::MpegTsAdapter;

streamhub::stream_writer!(MpegTsWriter, MpegTsAdapter, "mpegts");
