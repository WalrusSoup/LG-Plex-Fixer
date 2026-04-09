//! # plex-http-proxy
//!
//! An HTTP proxy that sits in front of Plex Media Server to work around broken
//! HLS transcoding on LG webOS TVs after the 10.2.2 firmware update.

use bytes::Bytes;
use colored::Colorize;
use futures_util::StreamExt;
use http_body_util::{combinators::BoxBody, BodyExt, Full, StreamBody};
use hyper::{body::Frame, server::conn::http1, service::service_fn, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{debug, error, info, trace, warn};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Session {
    dir: PathBuf,
    child: Option<tokio::process::Child>,
}

type Sessions = Arc<Mutex<HashMap<String, Session>>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Verbosity { Normal, Verbose, Trace }

struct Config {
    listen_port: u16,
    plex_backend: String,
    transcode_dir: String,
    ffmpeg: String,
    plex_exe: String,
    verbosity: Verbosity,
    no_manage: bool,
    log_dir: String,
}

impl Config {
    fn from_env_and_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let no_manage = args.iter().any(|a| a == "--no-manage");

        let verbosity = if args.iter().any(|a| a == "-vvv" || a == "-vv") {
            Verbosity::Trace
        } else if args.iter().any(|a| a == "-v" || a == "--verbose") {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        };

        let cli_val = |flag: &str| -> Option<String> {
            args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
        };

        let listen_port = cli_val("--port")
            .or_else(|| std::env::var("PLEX_PROXY_PORT").ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(32400);

        let plex_backend = cli_val("--backend")
            .or_else(|| std::env::var("PLEX_BACKEND").ok())
            .unwrap_or_else(|| "https://127.0.0.1:32401".to_string());

        let transcode_dir = cli_val("--transcode-dir")
            .or_else(|| std::env::var("PLEX_TRANSCODE_DIR").ok())
            .unwrap_or_else(default_transcode_dir);

        let ffmpeg = cli_val("--ffmpeg")
            .or_else(|| std::env::var("PLEX_FFMPEG").ok())
            .unwrap_or_else(|| "ffmpeg".to_string());

        let plex_exe = cli_val("--plex-exe")
            .or_else(|| std::env::var("PLEX_EXE").ok())
            .unwrap_or_else(default_plex_exe);

        let log_dir = cli_val("--log-dir")
            .or_else(|| std::env::var("PLEX_PROXY_LOG_DIR").ok())
            .unwrap_or_else(default_log_dir);

        Config { listen_port, plex_backend, transcode_dir, ffmpeg, plex_exe, verbosity, no_manage, log_dir }
    }
}

fn default_plex_exe() -> String {
    if cfg!(windows) {
        r"C:\Program Files\Plex\Plex Media Server\Plex Media Server.exe".to_string()
    } else if cfg!(target_os = "macos") {
        "/Applications/Plex Media Server.app/Contents/MacOS/Plex Media Server".to_string()
    } else {
        "plexmediaserver".to_string()
    }
}

fn default_transcode_dir() -> String {
    if cfg!(windows) {
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            return format!("{}\\Plex Media Server\\Cache\\Transcode\\NvencSessions", local);
        }
    }
    "/tmp/plex-nvenc-sessions".to_string()
}

fn default_log_dir() -> String {
    if cfg!(windows) {
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            return format!("{}\\lg-plex-fixer", local);
        }
    }
    "lg-plex-fixer".to_string()
}

// ---------------------------------------------------------------------------
// Logging setup
// ---------------------------------------------------------------------------

fn init_logging(config: &Config) {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let _ = std::fs::create_dir_all(&config.log_dir);

    let filter = match config.verbosity {
        Verbosity::Normal => "info",
        Verbosity::Verbose => "debug",
        Verbosity::Trace => "trace",
    };

    // File appender with rotation (new file every ~5MB-ish via daily + size check)
    let file_appender = tracing_appender::rolling::Builder::new()
        .filename_prefix("lg-plex-fixer")
        .filename_suffix("log")
        .max_log_files(5)
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .build(&config.log_dir)
        .expect("failed to create log appender");

    let (file_writer, _guard) = tracing_appender::non_blocking(file_appender);
    // Leak the guard so it lives for the duration of the program
    std::mem::forget(_guard);

    let file_layer = fmt::layer()
        .with_writer(file_writer)
        .with_ansi(false)
        .with_target(false);

    let console_layer = fmt::layer()
        .with_target(false)
        .with_ansi(true);

    tracing_subscriber::registry()
        .with(EnvFilter::new(filter))
        .with(console_layer)
        .with(file_layer)
        .init();
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

fn run_diagnostics(config: &Config) -> bool {
    println!("\n{}\n", "=== lg-plex-fixer startup ===".bold().cyan());
    let mut ok = true;

    // ffmpeg
    print!("  {} ffmpeg ({})... ", "●".bold().blue(), config.ffmpeg);
    match std::process::Command::new(&config.ffmpeg).arg("-version").output() {
        Ok(out) => {
            let ver = String::from_utf8_lossy(&out.stdout);
            let first_line = ver.lines().next().unwrap_or("unknown");
            println!("{} {}", "✓".bold().green(), first_line.trim().dimmed());
        }
        Err(_) => { println!("{}", "✗ not found".bold().red()); ok = false; }
    }

    // ffprobe
    let ffprobe = config.ffmpeg.replace("ffmpeg", "ffprobe");
    print!("  {} ffprobe ({ffprobe})... ", "●".bold().blue());
    match std::process::Command::new(&ffprobe).arg("-version").output() {
        Ok(_) => println!("{}", "✓".bold().green()),
        Err(_) => { println!("{}", "✗ not found".bold().red()); ok = false; }
    }

    // NVENC
    print!("  {} NVENC (h264_nvenc)... ", "●".bold().blue());
    match std::process::Command::new(&config.ffmpeg).args(["-hide_banner", "-encoders"]).output() {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            if text.contains("h264_nvenc") {
                println!("{}", "✓".bold().green());
            } else {
                println!("{}", "✗ h264_nvenc not in ffmpeg encoders".bold().red());
                ok = false;
            }
        }
        Err(_) => { println!("{}", "✗ could not query".bold().red()); ok = false; }
    }

    // libass
    print!("  {} libass (subtitles filter)... ", "●".bold().blue());
    match std::process::Command::new(&config.ffmpeg).args(["-hide_banner", "-filters"]).output() {
        Ok(out) => {
            let text = String::from_utf8_lossy(&out.stdout);
            if text.contains("subtitles") {
                println!("{}", "✓".bold().green());
            } else {
                println!("{}", "⚠ not found, sub burn-in may fail".bold().yellow());
            }
        }
        Err(_) => println!("{}", "⚠ could not query".bold().yellow()),
    }

    // Plex exe
    print!("  {} Plex ({})... ", "●".bold().blue(), config.plex_exe);
    if Path::new(&config.plex_exe).exists() {
        println!("{}", "✓".bold().green());
    } else if config.no_manage {
        println!("{}", "skip (--no-manage)".dimmed());
    } else {
        println!("{}", "⚠ not found, auto-start will fail".bold().yellow());
    }

    // Transcode dir
    print!("  {} transcode dir... ", "●".bold().blue());
    match std::fs::create_dir_all(&config.transcode_dir) {
        Ok(_) => println!("{}", "✓".bold().green()),
        Err(e) => { println!("{} {e}", "✗".bold().red()); ok = false; }
    }

    // Log dir
    print!("  {} log dir ({})... ", "●".bold().blue(), config.log_dir);
    match std::fs::create_dir_all(&config.log_dir) {
        Ok(_) => println!("{}", "✓".bold().green()),
        Err(e) => { println!("{} {e}", "✗".bold().red()); ok = false; }
    }

    println!();

    if !ok {
        eprintln!("{}", "  Some checks failed. Fix the issues above and try again.".bold().red());
    }
    ok
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    // Enable ANSI on Windows
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    let config = Config::from_env_and_args();

    if !run_diagnostics(&config) {
        std::process::exit(1);
    }

    init_logging(&config);

    // --- Plex orchestration ---
    if !config.no_manage {
        println!("  {} {}", "→".bold().cyan(), "stopping Plex processes...");
        if cfg!(windows) {
            for proc in &["Plex Media Server.exe", "PlexScriptHost.exe", "Plex Tuner Service.exe"] {
                let _ = std::process::Command::new("taskkill")
                    .args(["/F", "/IM", proc])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        } else {
            for pattern in &["Plex Media Server", "PlexScriptHost", "Plex Tuner Service"] {
                let _ = std::process::Command::new("pkill")
                    .args(["-f", pattern])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status();
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // Bind proxy port
    let addr = SocketAddr::from(([0, 0, 0, 0], config.listen_port));
    // Retry bind a few times — the port may take a moment to release after killing Plex
    let mut listener = None;
    for attempt in 0..10 {
        match TcpListener::bind(addr).await {
            Ok(l) => { listener = Some(l); break; }
            Err(_) if attempt < 9 => {
                if attempt == 0 {
                    print!("  {} waiting for port {}...", "→".bold().cyan(), config.listen_port);
                } else {
                    print!(".");
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(_) => {
                println!();
                eprintln!("{} failed to bind port {} after 10 attempts — is something else using it?",
                    "FATAL:".bold().red(), config.listen_port);
                std::process::exit(1);
            }
        }
    }
    if listener.is_some() && listener.as_ref().is_some() {
        // Print newline if we were printing dots
    }
    let listener = listener.unwrap();

    println!("  {} proxy bound to port {}", "→".bold().cyan(), config.listen_port.to_string().bold().green());

    // Start Plex
    if !config.no_manage {
        println!("  {} {}", "→".bold().cyan(), "starting Plex Media Server...");
        match std::process::Command::new(&config.plex_exe)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(_) => {
                print!("  {} waiting for Plex backend", "→".bold().cyan());
                let client_check = reqwest::Client::builder()
                    .danger_accept_invalid_certs(true)
                    .no_proxy()
                    .build()
                    .unwrap();
                let mut plex_up = false;
                for _ in 0..30 {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    print!(".");
                    if let Ok(resp) = client_check.get(format!("{}/identity", config.plex_backend)).send().await {
                        if resp.status().is_success() {
                            plex_up = true;
                            break;
                        }
                    }
                }
                if plex_up {
                    println!(" {}", "✓".bold().green());
                } else {
                    println!(" {}", "⚠ timeout (may still be starting)".bold().yellow());
                }
            }
            Err(e) => {
                println!("  {} failed to start Plex: {e}", "⚠".bold().yellow());
            }
        }
    }

    let verbosity = config.verbosity;
    let config = Arc::new(config);
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));

    println!("\n{}", "=== proxy ready ===".bold().green());
    println!("  {} http://0.0.0.0:{}", "listen:".dimmed(), config.listen_port);
    println!("  {} {}", "backend:".dimmed(), config.plex_backend);
    println!("  {} {}", "transcode:".dimmed(), config.transcode_dir);
    println!("  {} {}", "logs:".dimmed(), config.log_dir);
    println!("  {} {}", "verbosity:".dimmed(), match verbosity {
        Verbosity::Normal => "normal (use -v or -vvv for more)",
        Verbosity::Verbose => "verbose (-v)",
        Verbosity::Trace => "trace (-vvv)",
    });
    println!();

    info!("proxy started on port {}", config.listen_port);

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .no_proxy()
        .build()
        .expect("failed to build HTTP client");

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let client = client.clone();
        let sessions = sessions.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                handle(client.clone(), sessions.clone(), config.clone(), req)
            });
            if let Err(e) = http1::Builder::new()
                .keep_alive(true)
                .serve_connection(io, svc)
                .await
            {
                trace!("connection error: {e}");
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Query parsing helpers
// ---------------------------------------------------------------------------

fn parse_query(query: &str) -> HashMap<String, String> {
    query.split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let val = parts.next().unwrap_or("");
            Some((percent_decode(key), percent_decode(val)))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            if let Ok(s) = std::str::from_utf8(&[hi, lo]) {
                if let Ok(val) = u8::from_str_radix(s, 16) {
                    result.push(val as char);
                    continue;
                }
            }
            result.push('%');
            result.push(hi as char);
            result.push(lo as char);
        } else if b == b'+' {
            result.push(' ');
        } else {
            result.push(b as char);
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Request handler
// ---------------------------------------------------------------------------

async fn handle(
    client: reqwest::Client,
    sessions: Sessions,
    config: Arc<Config>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    let query = uri.query().unwrap_or("");

    // HLS segment requests
    if path.contains("/transcode/universal/session/") {
        let parts: Vec<&str> = path.split('/').collect();
        if let Some(pos) = parts.iter().position(|&p| p == "session") {
            if let Some(session_id) = parts.get(pos + 1) {
                let session_id = session_id.to_string();
                let rest: String = parts[pos + 2..].join("/");

                let sess = sessions.lock().await;
                if let Some(session) = sess.get(&session_id) {
                    let file_path = session.dir.join(&rest);
                    drop(sess);
                    debug!("serving segment: {rest}");
                    return serve_file(&file_path, &rest).await;
                }
            }
        }
        return proxy_to_plex(client, &config.plex_backend, req).await;
    }

    // Intercept start.m3u8
    if path.contains("/transcode/universal/start.m3u8") {
        let params = parse_query(query);
        let session_id = params.get("session").cloned().unwrap_or_else(|| {
            format!("nvenc-{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis())
        });
        let metadata_path = params.get("path").cloned().unwrap_or_default();
        let token = params.get("X-Plex-Token").cloned().unwrap_or_default();

        // Existing session — just serve manifest
        {
            let sess = sessions.lock().await;
            if let Some(session) = sess.get(&session_id) {
                let m3u8_path = session.dir.join("stream.m3u8");
                drop(sess);
                debug!(session_id, "session already running, serving manifest");
                return serve_m3u8(&session_id, &m3u8_path).await;
            }
        }

        info!(session_id, metadata_path, "new transcode session");

        match get_media_info(&client, &config, &metadata_path, &token).await {
            Ok((file_path, sub_index)) => {
                info!(file_path, ?sub_index, "media info resolved");

                let session_dir = PathBuf::from(&config.transcode_dir).join(&session_id);
                let _ = std::fs::create_dir_all(&session_dir);

                let child = start_nvenc_transcode(
                    &config.ffmpeg, &file_path, sub_index, &session_dir, config.verbosity,
                ).await;
                match child {
                    Ok(child) => {
                        let mut sess = sessions.lock().await;
                        sess.insert(session_id.clone(), Session {
                            dir: session_dir.clone(),
                            child: Some(child),
                        });
                        drop(sess);

                        let m3u8_path = session_dir.join("stream.m3u8");
                        for _ in 0..40 {
                            if m3u8_path.exists() {
                                let content = tokio::fs::read_to_string(&m3u8_path)
                                    .await.unwrap_or_default();
                                if content.contains("#EXTINF") { break; }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }

                        return serve_m3u8(&session_id, &m3u8_path).await;
                    }
                    Err(e) => error!("ffmpeg failed to start: {e}"),
                }
            }
            Err(e) => error!("failed to get media info: {e}"),
        }
        return proxy_to_plex(client, &config.plex_backend, req).await;
    }

    // Stop session
    if path.contains("/transcode/universal/stop") {
        let params = parse_query(query);
        if let Some(session_id) = params.get("session") {
            let mut sess = sessions.lock().await;
            if let Some(mut session) = sess.remove(session_id) {
                if let Some(ref mut child) = session.child {
                    let _ = child.kill().await;
                }
                let _ = std::fs::remove_dir_all(&session.dir);
                info!(session_id, "session stopped");
            }
        }
        let body = Full::new(Bytes::new()).map_err(|_| unreachable!()).boxed();
        return Ok(Response::builder().status(200).body(body).unwrap());
    }

    // Everything else → Plex
    proxy_to_plex(client, &config.plex_backend, req).await
}

// ---------------------------------------------------------------------------
// Plex metadata
// ---------------------------------------------------------------------------

async fn get_media_info(
    client: &reqwest::Client,
    config: &Config,
    metadata_path: &str,
    token: &str,
) -> Result<(String, Option<i32>), String> {
    let url = format!("{}{metadata_path}?X-Plex-Token={token}", config.plex_backend);
    let resp = client.get(&url)
        .header("Accept", "application/json")
        .send().await
        .map_err(|e| format!("request failed: {e}"))?;
    let text = resp.text().await.map_err(|e| format!("body read failed: {e}"))?;

    let mut file_path = String::new();
    if let Some(pos) = text.find("\"file\":\"") {
        let start = pos + 8;
        if let Some(end) = text[start..].find('"') {
            file_path = text[start..start + end].replace("\\\\", "\\").replace("\\/", "/");
        }
    }
    if file_path.is_empty() {
        return Err("could not find file path in metadata".to_string());
    }

    let sub_index = find_english_sub(&config.ffmpeg, &file_path).await;
    Ok((file_path, sub_index))
}

// ---------------------------------------------------------------------------
// Subtitle detection
// ---------------------------------------------------------------------------

async fn find_english_sub(ffmpeg_path: &str, file_path: &str) -> Option<i32> {
    let ffprobe = if ffmpeg_path.contains("ffmpeg") {
        ffmpeg_path.replace("ffmpeg", "ffprobe")
    } else {
        "ffprobe".to_string()
    };

    let output = tokio::process::Command::new(&ffprobe)
        .args(["-v", "quiet", "-print_format", "json", "-show_streams", "-select_streams", "s"])
        .arg(file_path)
        .output().await.ok()?;

    let text = String::from_utf8_lossy(&output.stdout);

    let mut first_sub: Option<i32> = None;
    let mut english_full: Option<i32> = None;
    let mut english_any: Option<i32> = None;

    let mut depth = 0;
    let mut block_start = 0;
    for (i, ch) in text.char_indices() {
        match ch {
            '{' => { if depth == 1 { block_start = i; } depth += 1; }
            '}' => {
                depth -= 1;
                if depth == 1 {
                    let block = &text[block_start..=i];
                    if !block.contains("\"subtitle\"") { continue; }

                    let idx = extract_json_int(block, "\"index\"");
                    let Some(idx) = idx else { continue };
                    let lang = extract_json_str(block, "\"language\"");
                    let title = extract_json_str(block, "\"title\"");

                    debug!(idx, ?lang, ?title, "found subtitle stream");

                    if first_sub.is_none() { first_sub = Some(idx); }
                    if lang.as_deref() == Some("eng") || lang.as_deref() == Some("English") {
                        if english_any.is_none() { english_any = Some(idx); }
                        let t = title.as_deref().unwrap_or("").to_lowercase();
                        if !t.contains("sign") && !t.contains("song") && !t.contains("forced") {
                            if english_full.is_none() { english_full = Some(idx); }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let result = english_full.or(english_any).or(first_sub);
    if let Some(idx) = result {
        info!(idx, "selected subtitle stream");
    } else {
        info!("no subtitle streams found");
    }
    result
}

fn extract_json_int(block: &str, key: &str) -> Option<i32> {
    let pos = block.find(key)?;
    let after = &block[pos + key.len()..];
    let num_start = after.find(|c: char| c.is_ascii_digit())?;
    let num_str: String = after[num_start..].chars().take_while(|c| c.is_ascii_digit()).collect();
    num_str.parse().ok()
}

fn extract_json_str(block: &str, key: &str) -> Option<String> {
    let pos = block.find(key)?;
    let after = &block[pos + key.len()..];
    let quote_start = after.find('"')? + 1;
    let quote_end = after[quote_start..].find('"')?;
    Some(after[quote_start..quote_start + quote_end].to_string())
}

// ---------------------------------------------------------------------------
// NVENC transcoding
// ---------------------------------------------------------------------------

async fn start_nvenc_transcode(
    ffmpeg: &str,
    file_path: &str,
    sub_index: Option<i32>,
    output_dir: &Path,
    verbosity: Verbosity,
) -> Result<tokio::process::Child, String> {
    let m3u8 = output_dir.join("stream.m3u8");
    let segment_pattern = output_dir.join("seg%05d.ts");

    // Extract subs to temp file (avoids path escaping hell)
    let sub_file = if let Some(si) = sub_index {
        let sub_path = output_dir.join("subs.ass");
        info!(si, path = %sub_path.display(), "extracting subtitles");
        let status = tokio::process::Command::new(ffmpeg)
            .args(["-i", file_path, "-map", &format!("0:{si}"), "-c:s", "copy", "-y"])
            .arg(sub_path.to_str().unwrap())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status().await;
        match status {
            Ok(s) if s.success() && sub_path.exists() => {
                info!("subtitle extraction OK");
                Some(sub_path)
            }
            _ => { warn!("subtitle extraction failed, continuing without subs"); None }
        }
    } else { None };

    let mut cmd = tokio::process::Command::new(ffmpeg);
    cmd.arg("-i").arg(file_path);

    if let Some(ref sub_path) = sub_file {
        let sub_str = sub_path.to_str().unwrap().replace('\\', "/").replace(":", "\\:");
        cmd.arg("-vf").arg(format!("subtitles='{}',format=yuv420p", sub_str));
    } else {
        cmd.arg("-vf").arg("format=yuv420p");
    }

    cmd.arg("-map").arg("0:v:0")
        .arg("-map").arg("0:a:0")
        .arg("-c:v").arg("h264_nvenc")
        .arg("-preset").arg("slow")
        .arg("-cq").arg("22")
        .arg("-c:a").arg("aac")
        .arg("-b:a").arg("192k")
        .arg("-f").arg("hls")
        .arg("-hls_time").arg("4")
        .arg("-hls_list_size").arg("0")
        .arg("-hls_flags").arg("independent_segments")
        .arg("-hls_segment_type").arg("mpegts")
        .arg("-hls_segment_filename").arg(segment_pattern.to_str().unwrap())
        .arg("-y")
        .arg(m3u8.to_str().unwrap());

    // Log the full ffmpeg command at verbose level
    debug!(
        command = format!("{} {}", ffmpeg, cmd.as_std().get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect::<Vec<_>>().join(" ")),
        "ffmpeg command"
    );

    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());

    info!("starting ffmpeg with NVENC");
    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;

    // Stream ffmpeg stderr based on verbosity
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            let reader = tokio::io::BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if verbosity == Verbosity::Trace {
                    trace!(target: "ffmpeg", "{line}");
                }
                // At verbose level, only log interesting lines
                else if verbosity == Verbosity::Verbose {
                    if line.contains("Error") || line.contains("error")
                        || line.contains("Opening") || line.contains("frame=")
                    {
                        debug!(target: "ffmpeg", "{line}");
                    }
                }
                // At normal level, only log errors
                else if line.contains("Error") || line.contains("error") || line.contains("Conversion failed") {
                    error!(target: "ffmpeg", "{line}");
                }
            }
        });
    }

    Ok(child)
}

// ---------------------------------------------------------------------------
// HLS serving
// ---------------------------------------------------------------------------

async fn serve_m3u8(
    session_id: &str,
    path: &Path,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => {
            let rewritten = content.lines().map(|line| {
                if line.starts_with("seg") && line.ends_with(".ts") {
                    format!("/video/:/transcode/universal/session/{session_id}/{line}")
                } else {
                    line.to_string()
                }
            }).collect::<Vec<_>>().join("\n");

            let body = Full::new(Bytes::from(rewritten)).map_err(|_| unreachable!()).boxed();
            Ok(Response::builder()
                .status(200)
                .header("content-type", "application/vnd.apple.mpegurl")
                .header("access-control-allow-origin", "*")
                .body(body).unwrap())
        }
        Err(_) => {
            let body = Full::new(Bytes::from("manifest not ready")).map_err(|_| unreachable!()).boxed();
            Ok(Response::builder().status(503).body(body).unwrap())
        }
    }
}

async fn serve_file(
    path: &Path,
    name: &str,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    for _ in 0..40 {
        if path.exists() { break; }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    match tokio::fs::read(path).await {
        Ok(data) => {
            let ct = if name.ends_with(".m3u8") { "application/vnd.apple.mpegurl" } else { "video/mp2t" };
            let body = Full::new(Bytes::from(data)).map_err(|_| unreachable!()).boxed();
            Ok(Response::builder()
                .status(200)
                .header("content-type", ct)
                .header("access-control-allow-origin", "*")
                .body(body).unwrap())
        }
        Err(_) => {
            let body = Full::new(Bytes::from("segment not found")).map_err(|_| unreachable!()).boxed();
            Ok(Response::builder().status(404).body(body).unwrap())
        }
    }
}

// ---------------------------------------------------------------------------
// Plex proxy passthrough
// ---------------------------------------------------------------------------

async fn proxy_to_plex(
    client: reqwest::Client,
    plex_backend: &str,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path_and_query = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let url = format!("{plex_backend}{path_and_query}");

    let is_transcode = path_and_query.contains("/transcode/universal/");

    let mut builder = client.request(method, &url);
    for (name, value) in req.headers() {
        // Strip oversized referer on transcode requests (causes 400s)
        if is_transcode && name.as_str().eq_ignore_ascii_case("referer") { continue; }
        builder = builder.header(name.as_str(), value.as_bytes());
    }

    let body_bytes = match req.collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => Bytes::new(),
    };
    if !body_bytes.is_empty() {
        builder = builder.body(body_bytes);
    }

    let resp = match builder.send().await {
        Ok(r) => r,
        Err(e) => {
            debug!("backend error: {e}");
            let body = Full::new(Bytes::from("bad gateway")).map_err(|_| unreachable!()).boxed();
            return Ok(Response::builder().status(StatusCode::BAD_GATEWAY).body(body).unwrap());
        }
    };

    let status = resp.status();
    let mut response = Response::builder().status(status.as_u16());
    for (name, value) in resp.headers() {
        response = response.header(name.as_str(), value.as_bytes());
    }

    let stream = resp.bytes_stream().map(|chunk| chunk.map(Frame::data));
    let body = BodyExt::boxed(StreamBody::new(stream));
    Ok(response.body(body).unwrap())
}
