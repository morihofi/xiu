use {
    axum::{
        body::Body,
        extract::{ConnectInfo, Request, State},
        handler::Handler,
        http::StatusCode,
        response::Response,
    },
    commonlib::auth::{Auth, SecretCarrier},
    futures::channel::mpsc::unbounded,
    httpflv::httpflv::HttpFlv,
    mpegts_proto::http::HttpTs,
    std::net::SocketAddr,
    streamhub::define::StreamHubEventSender,
    tokio::net::TcpListener,
};

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, GenericError>;
static NOTFOUND: &[u8] = b"Not Found";
static UNAUTHORIZED: &[u8] = b"Unauthorized";

async fn handle_connection(
    State((event_producer, auth, max_no_data_retries)): State<(
        StreamHubEventSender,
        Option<Auth>,
        usize,
    )>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    let path = req.uri().path();
    let query_string: Option<String> = req.uri().query().map(|s| s.to_string());
    log::debug!("http stream request: remote_addr={} uri={}", remote_addr, req.uri());

    // .flv
    if let Some(index) = path.find(".flv") {
        if index > 0 {
            let (left, _) = path.split_at(index);
            let rv: Vec<_> = left.split('/').collect();
            if rv.len() < 3 {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(NOTFOUND.into())
                    .unwrap();
            }
            let app_name = String::from(rv[1]);
            let stream_name = String::from(rv[2]);

            if let Some(auth_val) = auth.as_ref() {
                if auth_val
                    .authenticate(&stream_name, &query_string.map(SecretCarrier::Query), true)
                    .is_err()
                {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(UNAUTHORIZED.into())
                        .unwrap();
                }
            }

            let (http_response_data_producer, http_response_data_consumer) = unbounded();
            let mut flv_handler = HttpFlv::new(
                app_name.clone(),
                stream_name.clone(),
                event_producer,
                http_response_data_producer,
                req.uri().to_string(),
                remote_addr,
                max_no_data_retries,
            );
            log::info!(
                "httpflv subscribe: app={} stream={} remote_addr={}",
                app_name,
                stream_name,
                remote_addr
            );

            tokio::spawn(async move {
                if let Err(err) = flv_handler.run().await {
                    log::error!("flv handler run error {}", err);
                }
            });

            let mut resp = Response::new(Body::from_stream(http_response_data_consumer));
            resp.headers_mut()
                .insert("Access-Control-Allow-Origin", "*".parse().unwrap());
            return resp;
        }
    }

    // .ts (continuous)
    if let Some(index) = path.find(".ts") {
        if index > 0 {
            let (left, _) = path.split_at(index);
            let rv: Vec<_> = left.split('/').collect();
            if rv.len() < 3 {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(NOTFOUND.into())
                    .unwrap();
            }
            let app_name = String::from(rv[1]);
            let stream_name = String::from(rv[2]);

            if let Some(auth_val) = auth.as_ref() {
                if auth_val
                    .authenticate(&stream_name, &query_string.map(SecretCarrier::Query), true)
                    .is_err()
                {
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(UNAUTHORIZED.into())
                        .unwrap();
                }
            }

            let (http_response_data_producer, http_response_data_consumer) = unbounded();

            let mut ts_handler = HttpTs::new(
                app_name.clone(),
                stream_name.clone(),
                event_producer,
                http_response_data_producer,
                req.uri().to_string(),
                remote_addr,
                max_no_data_retries,
            );
            log::info!(
                "httpts subscribe: app={} stream={} remote_addr={}",
                app_name,
                stream_name,
                remote_addr
            );
            tokio::spawn(async move {
                if let Err(err) = ts_handler.run().await {
                    log::error!("ts handler run error {}", err);
                }
            });

            let mut resp = Response::new(Body::from_stream(http_response_data_consumer));
            resp.headers_mut()
                .insert("Access-Control-Allow-Origin", "*".parse().unwrap());
            resp.headers_mut()
                .insert("Content-Type", "video/mp2t".parse().unwrap());
            return resp;
        }
    }

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(NOTFOUND.into())
        .unwrap()
}

pub async fn run(
    event_producer: StreamHubEventSender,
    port: usize,
    auth: Option<Auth>,
    max_no_data_retries: usize,
) -> Result<()> {
    let listen_address = format!("0.0.0.0:{port}");
    let sock_addr: SocketAddr = listen_address.parse().unwrap();
    let listener = TcpListener::bind(sock_addr).await?;

    log::info!("HTTP stream server listening on http://{}", sock_addr);

    let handle_connection =
        handle_connection.with_state((event_producer.clone(), auth, max_no_data_retries));
    axum::serve(
        listener,
        handle_connection.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

