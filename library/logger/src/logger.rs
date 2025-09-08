use {
    super::target::FileTarget,
    anyhow::Result,
    chrono::prelude::*,
    env_logger::{Builder, Env, Target, WriteStyle},
    job_scheduler_ng::{Job, JobScheduler, Schedule},
    std::{
        env, fs,
        fs::{File, OpenOptions},
        path::Path,
        str::FromStr,
        sync::{
            mpsc::{channel, Receiver, Sender},
            Arc, Mutex,
        },
        thread,
        time::Duration,
    },
};
use std::io::Write as _;
#[cfg(feature = "serde_json")]
use serde_json::json;

#[derive(Clone, Debug, PartialEq)]
pub enum Rotate {
    Day,
    Hour,
    Minute,
}

impl FromStr for Rotate {
    type Err = ();
    fn from_str(input: &str) -> Result<Rotate, Self::Err> {
        match input {
            "day" => Ok(Rotate::Day),
            "hour" => Ok(Rotate::Hour),
            "minute" => Ok(Rotate::Minute),
            _ => Err(()),
        }
    }
}

fn get_log_file_name(rotate: Rotate) -> String {
    let local_time: DateTime<Local> = Local::now();
    match rotate {
        Rotate::Day => {
            format!(
                "{}{:02}{:02}0000",
                local_time.year(),
                local_time.month(),
                local_time.day(),
            )
        }
        Rotate::Hour => {
            format!(
                "{}{:02}{:02}{:02}00",
                local_time.year(),
                local_time.month(),
                local_time.day(),
                local_time.hour(),
            )
        }
        Rotate::Minute => {
            format!(
                "{}{:02}{:02}{:02}{:02}",
                local_time.year(),
                local_time.month(),
                local_time.day(),
                local_time.hour(),
                local_time.minute()
            )
        }
    }
}

const DEFAULT_SCHEDULER_RULE: &str = "0 * * * * *";

fn parse_scheduler_rule(rule: &str) -> Schedule {
    rule.parse().unwrap_or_else(|err| {
        log::error!(
            "invalid scheduler rule: {rule}, err: {err}; using default rule '{DEFAULT_SCHEDULER_RULE}'"
        );
        DEFAULT_SCHEDULER_RULE
            .parse()
            .expect("default scheduler rule should be valid")
    })
}

pub fn gen_log_file(rotate: Rotate, path: String) -> Result<File> {
    let file_name = get_log_file_name(rotate);
    let full_path = format!("{path}/{file_name}.log");
    // println!("file_name: {}", full_path);
    if !Path::new(&full_path).exists() {
        //println!("create file : {}", full_path);
        Ok(File::create(full_path)?)
    } else {
        //println!("open file : {}", full_path);
        let file = OpenOptions::new().append(true).open(full_path)?;
        Ok(file)
    }
}

pub fn gen_log_file_thread_run(
    file_handler: Arc<Mutex<File>>,
    rotate: Rotate,
    path: String,
    r: Receiver<bool>,
) {
    thread::spawn(move || {
        let mut sched = JobScheduler::new();

        let scheduler_rule = match rotate {
            Rotate::Minute => "0 * * * * *",
            Rotate::Hour => "0 0 * * * *",
            Rotate::Day => "0 0 0 * * *",
        };

        sched.add(Job::new(parse_scheduler_rule(scheduler_rule), || {
            let dt: DateTime<Local> = Local::now();

            let cur_number = format!(
                "{}-{:02}-{:02} {:02}:{:02}:00",
                dt.year(),
                dt.month(),
                dt.day(),
                dt.hour(),
                dt.minute()
            );
            log::debug!("log rotation tick at {cur_number}");

            match gen_log_file(rotate.to_owned(), path.to_owned()) {
                Ok(file) => {
                    let mut state = file_handler.lock().expect("Could not lock mutex");
                    *state = file;
                }
                Err(err) => {
                    log::error!("gen_log_file err : {err}");
                }
            }
        }));
        let duration = Duration::from_millis(500);
        loop {
            sched.tick();
            if r.recv_timeout(duration).is_ok() {
                return;
            }
        }
    });
}
#[derive(Default)]
pub struct Logger {
    close_sender: Option<Sender<bool>>,
}

impl Logger {
    pub fn new(level: &String, rotate: Option<Rotate>, path: Option<String>) -> Result<Logger> {
        // Respect existing RUST_LOG if set; otherwise apply provided level
        let env = Env::default().filter_or("RUST_LOG", level);

        // Common formatter: text or json driven via XIU_LOG_FORMAT env var (text|json)
        let is_json = match env::var("XIU_LOG_FORMAT") {
            Ok(val) => val.eq_ignore_ascii_case("json"),
            Err(_) => false,
        };

        // Console sink when no file rotate/path specified
        if rotate.is_none() || path.is_none() {
            let style = match env::var("XIU_LOG_STYLE").unwrap_or_else(|_| "auto".to_string()).as_str() {
                "always" => WriteStyle::Always,
                "never" => WriteStyle::Never,
                _ => WriteStyle::Auto,
            };

            let mut builder = Builder::from_env(env);
            builder.write_style(style);
            if is_json {
                builder.format(|buf, record| {
                    let ts = buf.timestamp_millis();
                    let thread = format!("{:?}", thread::current().id());
                    #[cfg(feature = "serde_json")]
                    {
                        let payload = json!({
                            "ts": ts.to_string(),
                            "level": record.level().to_string(),
                            "target": record.target(),
                            "module": record.module_path().unwrap_or(""),
                            "file": record.file().unwrap_or(""),
                            "line": record.line().unwrap_or(0),
                            "thread": thread,
                            "msg": record.args().to_string(),
                        });
                        return writeln!(buf, "{}", payload.to_string());
                    }
                    // Fallback to text if feature is off
                    writeln!(
                        buf,
                        "{ts} {} {}:{}:{} - {}",
                        record.level(),
                        record.module_path().unwrap_or(""),
                        record.file().unwrap_or(""),
                        record.line().unwrap_or(0),
                        record.args()
                    )
                });
            } else {
                builder.format(|buf, record| {
                    let ts = buf.timestamp_millis();
                    let lvl = buf.default_styled_level(record.level());
                    let thread = format!("{:?}", thread::current().id());
                    writeln!(
                        buf,
                        "{ts} {lvl} [{thread}] {} {}:{} - {}",
                        record.target(),
                        record.file().unwrap_or(""),
                        record.line().unwrap_or(0),
                        record.args()
                    )
                });
            }
            builder.target(Target::Stderr).init();
            return Ok(Self { ..Default::default() });
        }

        // File sink with rotation
        let path_val = path.unwrap();
        let rotate_val = rotate.unwrap();

        if let Err(err) = fs::create_dir_all(path_val.clone()) {
            log::error!("cannot create folder: {path_val}, err: {err}");
        }
        let file = gen_log_file(rotate_val.clone(), path_val.clone())?;
        let target = FileTarget::new(file)?;

        let handler = target.cur_file_handler.clone();
        let (send, receiver) = channel::<bool>();
        gen_log_file_thread_run(handler, rotate_val, path_val, receiver);

        let mut builder = Builder::from_env(env);
        // Never emit ANSI escape codes to files
        builder.write_style(WriteStyle::Never);
        if is_json {
            builder.format(|buf, record| {
                let ts = buf.timestamp_millis();
                let thread = format!("{:?}", thread::current().id());
                #[cfg(feature = "serde_json")]
                {
                    let payload = json!({
                        "ts": ts.to_string(),
                        "level": record.level().to_string(),
                        "target": record.target(),
                        "module": record.module_path().unwrap_or(""),
                        "file": record.file().unwrap_or(""),
                        "line": record.line().unwrap_or(0),
                        "thread": thread,
                        "msg": record.args().to_string(),
                    });
                    return writeln!(buf, "{}", payload.to_string());
                }
                writeln!(
                    buf,
                    "{ts} {} {}:{}:{} - {}",
                    record.level(),
                    record.module_path().unwrap_or(""),
                    record.file().unwrap_or(""),
                    record.line().unwrap_or(0),
                    record.args()
                )
            });
        } else {
            builder.format(|buf, record| {
                let ts = buf.timestamp_millis();
                let lvl = record.level();
                let thread = format!("{:?}", thread::current().id());
                writeln!(
                    buf,
                    "{ts} {lvl} [{thread}] {} {}:{} - {}",
                    record.target(),
                    record.file().unwrap_or(""),
                    record.line().unwrap_or(0),
                    record.args()
                )
            });
        }
        builder.target(Target::Pipe(Box::new(target))).init();

        Ok(Self { close_sender: Some(send) })
    }
    pub fn stop(&self) {
        if let Some(sender) = &self.close_sender {
            if let Err(err) = sender.send(true) {
                log::error!("Logger close err :{err}");
            }
        }
    }
}
#[cfg(test)]
mod tests {

    use super::{parse_scheduler_rule, Logger, Rotate, DEFAULT_SCHEDULER_RULE};
    use chrono::Utc;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::Duration;

    #[test]
    fn test_log() {
        let logger = Logger::new(
            &String::from("info"),
            Some(Rotate::Minute),
            Some(String::from("./logs")),
        )
        .unwrap();

        let mut recorder = 0;

        loop {
            std::thread::sleep(Duration::from_millis(500));
            log::trace!("some trace log");
            log::debug!("some debug log");
            log::info!("some information log");
            log::warn!("some warning log");
            log::error!("some error log");
            recorder += 1;
            if recorder > 10 {
                logger.stop();
                break;
            }
        }
    }
    #[test]
    fn test_write_file() {
        match OpenOptions::new().append(true).open("abc.txt") {
            Ok(mut file) => {
                if let Err(err) = file.write_all(&[b'h', b'e', b'l', b'l', b'o', b'o']) {
                    log::error!("file write_all: {err}");
                }
            }
            Err(err) => {
                log::error!("file create: {err}");
            }
        }
    }

    #[test]
    fn test_invalid_scheduler_rule_falls_back_to_default() {
        let default_schedule = parse_scheduler_rule(DEFAULT_SCHEDULER_RULE);
        let invalid_schedule = parse_scheduler_rule("invalid");

        let default_next = default_schedule.upcoming(Utc).next();
        let invalid_next = invalid_schedule.upcoming(Utc).next();

        assert_eq!(default_next, invalid_next);
    }
}
