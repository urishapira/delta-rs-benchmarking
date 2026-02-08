use cpu_time::ProcessTime;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, System};

#[derive(Debug, Clone, Copy)]
pub struct Metrics {
    pub wall_ms: u128,
    pub cpu_ms: u128,
    pub peak_rss_mib: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    WithFiles,
    WithoutFiles,
    Both,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "with_files" => Ok(Mode::WithFiles),
            "without_files" => Ok(Mode::WithoutFiles),
            "both" => Ok(Mode::Both),
            _ => Err(format!(
                "Invalid --mode '{}'. Expected: with_files | without_files | both",
                s
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Mode::WithFiles => "with_files",
            Mode::WithoutFiles => "without_files",
            Mode::Both => "both",
        }
    }
}

pub struct Args {
    pub table_path: PathBuf,
    pub version: Option<i64>,
    pub mode: Mode,
    pub interval_ms: u64,
}

/// Implemented by each binary crate (old/new) using its own deltalake version.
#[async_trait::async_trait]
pub trait DeltaLoader {
    /// Discover latest version if `--version` not provided.
    async fn discover_version(&self, table_url: url::Url) -> Result<i64, String>;

    /// Load the table at a version. `require_files=true` is "with_files".
    async fn load(&self, table_url: url::Url, version: i64, require_files: bool)
        -> Result<(), String>;
}

struct MemorySampler {
    stop: Arc<AtomicBool>,
    peak_bytes: Arc<AtomicU64>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MemorySampler {
    fn start(pid: Pid, interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_bytes = Arc::new(AtomicU64::new(0));

        let stop2 = Arc::clone(&stop);
        let peak2 = Arc::clone(&peak_bytes);

        let handle = thread::spawn(move || {
            let mut sys = System::new();
            while !stop2.load(Ordering::Relaxed) {
                sys.refresh_process(pid);
                if let Some(p) = sys.process(pid) {
                    let rss_bytes = p.memory() as u64;
                    loop {
                        let prev = peak2.load(Ordering::Relaxed);
                        if rss_bytes <= prev {
                            break;
                        }
                        if peak2
                            .compare_exchange(prev, rss_bytes, Ordering::Relaxed, Ordering::Relaxed)
                            .is_ok()
                        {
                            break;
                        }
                    }
                }
                thread::sleep(interval);
            }
            sys.refresh_process(pid);
            if let Some(p) = sys.process(pid) {
                let rss_bytes = p.memory() as u64;
                loop {
                    let prev = peak2.load(Ordering::Relaxed);
                    if rss_bytes <= prev {
                        break;
                    }
                    if peak2
                        .compare_exchange(prev, rss_bytes, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    {
                        break;
                    }
                }
            }
        });

        Self {
            stop,
            peak_bytes,
            handle: Some(handle),
        }
    }

    fn stop(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        self.peak_bytes.load(Ordering::Relaxed)
    }
}

pub fn path_to_file_url(p: &PathBuf) -> Result<url::Url, String> {
    url::Url::from_file_path(p).map_err(|_| format!("Failed converting path to file:// URL: {p:?}"))
}

pub fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);

    let table_path = args
        .next()
        .ok_or_else(|| "Missing required arg: <TABLE_PATH>".to_string())?;

    let mut version: Option<i64> = None;
    let mut mode: Mode = Mode::Both;
    let mut interval_ms: u64 = 10;

    while let Some(a) = args.next() {
        match a.as_str() {
            "--version" => {
                let v = args
                    .next()
                    .ok_or_else(|| "Missing value after --version".to_string())?;
                version = Some(v.parse::<i64>().map_err(|e| format!("Bad --version: {e}"))?);
            }
            "--mode" => {
                let m = args
                    .next()
                    .ok_or_else(|| "Missing value after --mode".to_string())?;
                mode = Mode::parse(&m)?;
            }
            "--interval-ms" => {
                let v = args
                    .next()
                    .ok_or_else(|| "Missing value after --interval-ms".to_string())?;
                interval_ms = v
                    .parse::<u64>()
                    .map_err(|e| format!("Bad --interval-ms: {e}"))?;
            }
            "-h" | "--help" => {
                return Err(
                    "Args: <TABLE_PATH> [--version <i64>] [--mode with_files|without_files|both] [--interval-ms <u64>]"
                        .to_string(),
                )
            }
            _ => return Err(format!("Unknown arg: {a}")),
        }
    }

    Ok(Args {
        table_path: PathBuf::from(table_path),
        version,
        mode,
        interval_ms,
    })
}

async fn run_one(
    loader: &dyn DeltaLoader,
    label: &str,
    table_url: url::Url,
    version: i64,
    require_files: bool,
    interval_ms: u64,
) -> Result<Metrics, String> {
    let pid = Pid::from_u32(std::process::id());
    let sampler = MemorySampler::start(pid, Duration::from_millis(interval_ms));

    let wall_start = Instant::now();
    let cpu_start = ProcessTime::now();

    loader
        .load(table_url, version, require_files)
        .await
        .map_err(|e| format!("Load failed for {label}: {e}"))?;

    let wall_ms = wall_start.elapsed().as_millis();
    let cpu_ms = cpu_start.elapsed().as_millis();

    let peak_bytes = sampler.stop();
    let peak_rss_mib = (peak_bytes as f64) / (1024.0 * 1024.0);

    Ok(Metrics {
        wall_ms,
        cpu_ms,
        peak_rss_mib,
    })
}

pub async fn run_benchmark(name: &str, loader: &dyn DeltaLoader) -> Result<(), String> {
    let args = parse_args()?;
    let table_url = path_to_file_url(&args.table_path)?;

    let version = match args.version {
        Some(v) => v,
        None => loader.discover_version(table_url.clone()).await?,
    };

    println!("Delta load benchmark ({name})");
    println!("  path           : {:?}", args.table_path);
    println!("  table_url      : {}", table_url);
    println!("  version        : {}", version);
    println!("  mode           : {}", args.mode.as_str());
    println!("  sample interval: {} ms", args.interval_ms);
    println!();

    match args.mode {
        Mode::WithFiles => {
            let m = run_one(loader, "with_files", table_url, version, true, args.interval_ms).await?;
            println!("Results:");
            println!(
                "  {:<14} wall_ms={:<8} cpu_ms={:<8} peak_rss_mib={:<10.2}",
                "with_files", m.wall_ms, m.cpu_ms, m.peak_rss_mib
            );
        }
        Mode::WithoutFiles => {
            let m =
                run_one(loader, "without_files", table_url, version, false, args.interval_ms).await?;
            println!("Results:");
            println!(
                "  {:<14} wall_ms={:<8} cpu_ms={:<8} peak_rss_mib={:<10.2}",
                "without_files", m.wall_ms, m.cpu_ms, m.peak_rss_mib
            );
        }
        Mode::Both => {
            let m_with =
                run_one(loader, "with_files", table_url.clone(), version, true, args.interval_ms).await?;
            let m_without =
                run_one(loader, "without_files", table_url.clone(), version, false, args.interval_ms).await?;
            println!("Results:");
            println!(
                "  {:<14} wall_ms={:<8} cpu_ms={:<8} peak_rss_mib={:<10.2}",
                "with_files", m_with.wall_ms, m_with.cpu_ms, m_with.peak_rss_mib
            );
            println!(
                "  {:<14} wall_ms={:<8} cpu_ms={:<8} peak_rss_mib={:<10.2}",
                "without_files", m_without.wall_ms, m_without.cpu_ms, m_without.peak_rss_mib
            );
            println!();
            println!("Tip: for clean A/B, run each mode in a separate process.");
        }
    }

    Ok(())
}