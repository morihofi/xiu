use {
    super::{errors::HlsError, flv_data_receiver::FlvDataReceiver},
    streamhub::{
        define::{BroadcastEvent, BroadcastEventReceiver, StreamHubEventSender},
        stream::StreamIdentifier,
    },
};

pub struct HlsRemuxer {
    client_event_consumer: BroadcastEventReceiver,
    event_producer: StreamHubEventSender,
    need_record: bool,
    data_dir: Option<String>,
    max_no_data_retries: usize,
    no_data_sleep_ms: u64,
}

impl HlsRemuxer {
    pub fn new(
        consumer: BroadcastEventReceiver,
        event_producer: StreamHubEventSender,
        need_record: bool,
        data_dir: Option<String>,
        max_no_data_retries: usize,
        no_data_sleep_ms: u64,
    ) -> Self {
        Self {
            client_event_consumer: consumer,
            event_producer,
            need_record,
            data_dir,
            max_no_data_retries,
            no_data_sleep_ms,
        }
    }

    pub async fn run(&mut self) -> Result<(), HlsError> {
        loop {
            let val = self.client_event_consumer.recv().await?;
            match val {
                BroadcastEvent::Publish { identifier } => {
                    if let StreamIdentifier::Rtmp {
                        app_name,
                        stream_name,
                    } = identifier
                    {
                        let mut rtmp_subscriber = FlvDataReceiver::new(
                            app_name,
                            stream_name,
                            self.event_producer.clone(),
                            5,
                            self.need_record,
                            self.data_dir.clone(),
                            self.max_no_data_retries,
                            self.no_data_sleep_ms,
                        );

                        tokio::spawn(async move {
                            if let Err(err) = rtmp_subscriber.run().await {
                                println!("hls handler run error {err}");
                            }
                        });
                    }
                }
                _ => {
                    log::trace!("other infos...");
                }
            }
        }
    }
}
