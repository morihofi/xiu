use streamhub::define::StreamHubEventSender;

use super::session::server_session;
use commonlib::auth::Auth;
use std::net::SocketAddr;
use tokio::io::Error;
use tokio::net::TcpListener;

pub struct RtmpServer {
    address: String,
    event_producer: StreamHubEventSender,
    gop_num: usize,
    auth: Option<Auth>,
    read_timeout_ms: u64,
    max_no_data_retries: usize,
}

impl RtmpServer {
    pub fn new(
        address: String,
        event_producer: StreamHubEventSender,
        gop_num: usize,
        auth: Option<Auth>,
        read_timeout_ms: u64,
        max_no_data_retries: usize,
    ) -> Self {
        Self {
            address,
            event_producer,
            gop_num,
            auth,
            read_timeout_ms,
            max_no_data_retries,
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        let socket_addr: &SocketAddr = &self.address.parse().unwrap();
        let listener = TcpListener::bind(socket_addr).await?;

        log::info!("Rtmp server listening on tcp://{}", socket_addr);
        loop {
            let (tcp_stream, _) = listener.accept().await?;
            //tcp_stream.set_keepalive(Some(Duration::from_secs(30)))?;

            let mut session = server_session::ServerSession::new(
                tcp_stream,
                self.event_producer.clone(),
                self.gop_num,
                self.auth.clone(),
                self.read_timeout_ms,
                self.max_no_data_retries,
            );
            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    log::info!(
                        "session run error: session_type: {}, app_name: {}, stream_name: {}, err: {}",
                        session.common.session_type,
                        session.app_name,
                        session.stream_name,
                        err
                    );
                }
            });
        }
    }
}
