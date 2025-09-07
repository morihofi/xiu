use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct StreamKey {
    pub app_name: String,
    pub stream_name: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub enum StreamIdentifier {
    #[default]
    Unknown,
    #[serde(rename = "rtmp")]
    Rtmp {
        app_name: String,
        stream_name: String,
    },
    #[serde(rename = "rtsp")]
    Rtsp { stream_path: String },
    #[serde(rename = "webrtc")]
    WebRTC {
        app_name: String,
        stream_name: String,
    },
}
impl fmt::Display for StreamIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StreamIdentifier::Rtmp {
                app_name,
                stream_name,
            } => {
                write!(f, "RTMP - app_name: {app_name}, stream_name: {stream_name}")
            }
            StreamIdentifier::Rtsp {
                stream_path: stream_name,
            } => {
                write!(f, "RTSP - stream_name: {stream_name}")
            }
            StreamIdentifier::WebRTC {
                app_name,
                stream_name,
            } => {
                write!(
                    f,
                    "WebRTC - app_name: {app_name}, stream_name: {stream_name}"
                )
            }
            StreamIdentifier::Unknown => {
                write!(f, "Unknown")
            }
        }
    }
}

impl StreamIdentifier {
    pub fn to_key(&self) -> Option<StreamKey> {
        match self {
            StreamIdentifier::Rtmp { app_name, stream_name }
            | StreamIdentifier::WebRTC { app_name, stream_name } => Some(StreamKey {
                app_name: app_name.clone(),
                stream_name: stream_name.clone(),
            }),
            StreamIdentifier::Rtsp { stream_path } => {
                stream_path.split_once('/').map(|(app, stream)| StreamKey {
                    app_name: app.to_string(),
                    stream_name: stream.to_string(),
                })
            }
            StreamIdentifier::Unknown => None,
        }
    }
}
