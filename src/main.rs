//! # plex-http-proxy
//!
//! An HTTP proxy that sits in front of Plex Media Server to work around broken
//! HLS transcoding on LG webOS TVs after the 10.2.2 firmware update.
//!
//! The proxy:
//! - Listens on the Plex port (default 32400) and forwards most requests to the
//!   real Plex backend (default <https://127.0.0.1:32401>).
//! - Terminates TLS so the webOS native HLS player can fetch segments over plain
//!   HTTP (works around the broken TLS stack in webOS 10.2.2).
//! - Intercepts transcode `start.m3u8` requests and runs ffmpeg with NVENC
//!   instead of Plex's built-in transcoder, producing HLS segments that the
//!   Chrome 120 MSE engine on webOS can actually play (works around
//!   MEDIA_ELEMENT_ERROR code 4 on MPEGTS segments).

use bytes::Bytes;
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

/// Active transcoding session state.
struct Session {
    /// Directory containing HLS segments and the manifest for this session.
    dir: PathBuf,
    /// Handle to the running ffmpeg child process.
    child: Option<tokio::process::Child>,
}

/// Thread-safe map of session ID to active session.
type Sessions = Arc<Mutex<HashMap<String, Session>>>;

/// Runtime configuration, built from environment variables, CLI args, or defaults.
struct Config {
    /// Port the proxy listens on (default: 32400).
    listen_port: u16,
    /// URL of the real Plex Media Server backend (default: https://127.0.0.1:32401).
    plex_backend: String,
    /// Directory where NVENC transcode sessions write HLS output.
    transcode_dir: String,
    /// Path to the ffmpeg binary (default: "ffmpeg", found via PATH).
    ffmpeg: String,
    /// Print connection-level errors to stderr.
    verbose: bool,
}

impl Config {
    /// Build configuration from environment variables and CLI arguments.
    ///
    /// Environment variables (override defaults):
    ///   - `PLEX_PROXY_PORT` — listen port
    ///   - `PLEX_BACKEND` — backend URL
    ///   - `PLEX_TRANSCODE_DIR` — transcode output directory
    ///   - `PLEX_FFMPEG` — path to ffmpeg binary
    ///
    /// CLI arguments (override environment):
    ///   - `--port <PORT>`
    ///   - `--backend <URL>`
    ///   - `--transcode-dir <PATH>`
    ///   - `--ffmpeg <PATH>`
    ///   - `--verbose` / `-v`
    fn from_env_and_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let verbose = args.iter().any(|a| a == "--verbose" || a == "-v");

        let cli_val = |flag: &str| -> Option<String> {
            args.windows(2)
                .find(|w| w[0] == flag)
                .map(|w| w[1].clone())
        };

        let listen_port = cli_val("--port")
            .or_else(|| std::env::var("PLEX_PROXY_PORT").ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(32400);

        let plex_backend = cli_val("--backend")
            .or_else(|| std::env::var("PLEX_BACKEND").ok())
            .unwrap_or_else(|| "https://127.0.0.1:32401".to_string());

        let default_transcode_dir = default_transcode_dir();
        let transcode_dir = cli_val("--transcode-dir")
            .or_else(|| std::env::var("PLEX_TRANSCODE_DIR").ok())
            .unwrap_or(default_transcode_dir);

        let ffmpeg = cli_val("--ffmpeg")
            .or_else(|| std::env::var("PLEX_FFMPEG").ok())
            .unwrap_or_else(|| "ffmpeg".to_string());

        Config { listen_port, plex_backend, transcode_dir, ffmpeg, verbose }
    }
}

/// Returns a platform-appropriate default transcode directory.
fn default_transcode_dir() -> String {
    if cfg!(windows) {
        if let Ok(local) = std::env::var("LOCALAPPDATA") {
            return format!("{}\\Plex Media Server\\Cache\\Transcode\\NvencSessions", local);
        }
    }
    // Fallback for Linux / macOS / unknown
    "/tmp/plex-nvenc-sessions".to_string()
}

#[tokio::main]
async fn main() {
    let config = Arc::new(Config::from_env_and_args());
    let addr = SocketAddr::from(([0, 0, 0, 0], config.listen_port));
    let listener = TcpListener::bind(addr)
        .await
        .unwrap_or_else(|_| panic!(
            "failed to bind port {} -- is Plex already running on this port? Stop it first.",
            config.listen_port
        ));
    let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));

    println!("plex-http-proxy listening on http://0.0.0.0:{}", config.listen_port);
    println!("proxying to {}", config.plex_backend);
    println!("transcode dir: {}", config.transcode_dir);
    println!("NVENC transcoding enabled -- subtitle burn-in handled locally");

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .no_proxy()
        .build()
        .expect("failed to build HTTP client");

    // Ensure transcode dir exists
    let _ = std::fs::create_dir_all(&config.transcode_dir);

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(s) => s,
            Err(_) => continue,
        };
        let client = client.clone();
        let sessions = sessions.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let verbose = config.verbose;
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                handle(client.clone(), sessions.clone(), config.clone(), req)
            });
            if let Err(e) = http1::Builder::new()
                .keep_alive(true)
                .serve_connection(io, svc)
                .await
            {
                if verbose {
                    eprintln!("conn error: {e}");
                }
            }
        });
    }
}

/// Parse a URL query string into key-value pairs with percent-decoding.
fn parse_query(query: &str) -> HashMap<String, String> {
    query.split('&')
        .filter_map(|pair| {
            let mut parts = pair.splitn(2, '=');
            let key = parts.next()?;
            let val = parts.next().unwrap_or("");
            Some((
                percent_decode(key),
                percent_decode(val),
            ))
        })
        .collect()
}

/// Decode percent-encoded bytes and `+` as space.
fn percent_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let hi = chars.next().unwrap_or(b'0');
            let lo = chars.next().unwrap_or(b'0');
            let hex = [hi, lo];
            if let Ok(s) = std::str::from_utf8(&hex) {
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

/// Main request handler. Decides whether to intercept (HLS segments, transcode
/// start/stop) or proxy the request straight through to Plex.
async fn handle(
    client: reqwest::Client,
    sessions: Sessions,
    config: Arc<Config>,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    let uri = req.uri().clone();
    let path = uri.path();
    let query = uri.query().unwrap_or("");

    // HLS segment requests: /video/:/transcode/universal/session/{id}/...
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
                    return serve_file(&file_path, &rest).await;
                }
            }
        }
        return proxy_to_plex(client, &config.plex_backend, req).await;
    }

    // Intercept start.m3u8 -- this is where we take over transcoding
    if path.contains("/transcode/universal/start.m3u8") {
        let params = parse_query(query);
        let session_id = params.get("session").cloned().unwrap_or_else(|| {
            format!("nvenc-{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis())
        });
        let metadata_path = params.get("path").cloned().unwrap_or_default();
        let token = params.get("X-Plex-Token").cloned().unwrap_or_default();

        // Check if session already exists -- just serve the manifest
        {
            let sess = sessions.lock().await;
            if let Some(session) = sess.get(&session_id) {
                let m3u8_path = session.dir.join("stream.m3u8");
                drop(sess);
                eprintln!("[NVENC] session {session_id} already running, serving manifest");
                return serve_m3u8(&session_id, &m3u8_path).await;
            }
        }

        eprintln!("[NVENC] start.m3u8 for {metadata_path} session={session_id}");

        // Get the actual file path from Plex metadata
        match get_media_info(&client, &config, &metadata_path, &token).await {
            Ok((file_path, sub_index)) => {
                eprintln!("[NVENC] file: {file_path}");
                eprintln!("[NVENC] subtitle index: {sub_index:?}");

                let session_dir = PathBuf::from(&config.transcode_dir).join(&session_id);
                let _ = std::fs::create_dir_all(&session_dir);

                // Start ffmpeg with NVENC
                let child = start_nvenc_transcode(
                    &config.ffmpeg, &file_path, sub_index, &session_dir,
                ).await;
                match child {
                    Ok(child) => {
                        let mut sess = sessions.lock().await;
                        sess.insert(session_id.clone(), Session {
                            dir: session_dir.clone(),
                            child: Some(child),
                        });
                        drop(sess);

                        // Wait for ffmpeg to produce the first segment (up to 10s)
                        let m3u8_path = session_dir.join("stream.m3u8");
                        for _ in 0..40 {
                            if m3u8_path.exists() {
                                let content = tokio::fs::read_to_string(&m3u8_path)
                                    .await
                                    .unwrap_or_default();
                                if content.contains("#EXTINF") {
                                    break;
                                }
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }

                        return serve_m3u8(&session_id, &m3u8_path).await;
                    }
                    Err(e) => {
                        eprintln!("[NVENC] ffmpeg failed to start: {e}");
                    }
                }
            }
            Err(e) => {
                eprintln!("[NVENC] failed to get media info: {e}");
            }
        }
        // Fall through to Plex transcoder on failure
        return proxy_to_plex(client, &config.plex_backend, req).await;
    }

    // Transcode stop -- clean up session
    if path.contains("/transcode/universal/stop") {
        let params = parse_query(query);
        if let Some(session_id) = params.get("session") {
            let mut sess = sessions.lock().await;
            if let Some(mut session) = sess.remove(session_id) {
                if let Some(ref mut child) = session.child {
                    let _ = child.kill().await;
                }
                let _ = std::fs::remove_dir_all(&session.dir);
                eprintln!("[NVENC] stopped session {session_id}");
            }
        }
        let body = Full::new(Bytes::new()).map_err(|_| unreachable!()).boxed();
        return Ok(Response::builder().status(200).body(body).unwrap());
    }

    // Everything else passes through to Plex unchanged
    proxy_to_plex(client, &config.plex_backend, req).await
}

/// Fetch media metadata from Plex and extract the file path and subtitle stream index.
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

    // Extract file path from JSON response
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

    // Use ffprobe to find an English subtitle stream for burn-in
    let sub_index = find_english_sub(&config.ffmpeg, &file_path).await;
    Ok((file_path, sub_index))
}

/// Use ffprobe to scan for subtitle streams, preferring English.
/// Returns the absolute stream index suitable for `-map 0:{index}`.
async fn find_english_sub(ffmpeg_path: &str, file_path: &str) -> Option<i32> {
    // Derive ffprobe path from ffmpeg path
    let ffprobe = if ffmpeg_path.contains("ffmpeg") {
        ffmpeg_path.replace("ffmpeg", "ffprobe")
    } else {
        "ffprobe".to_string()
    };

    let output = tokio::process::Command::new(&ffprobe)
        .arg("-v").arg("quiet")
        .arg("-print_format").arg("json")
        .arg("-show_streams")
        .arg("-select_streams").arg("s")
        .arg(file_path)
        .output()
        .await
        .ok()?;

    let text = String::from_utf8_lossy(&output.stdout);

    // Split JSON into individual stream objects and parse each one
    let mut first_sub: Option<i32> = None;
    let mut english_full: Option<i32> = None;
    let mut english_any: Option<i32> = None;

    // Find each stream block between { }
    let mut depth = 0;
    let mut block_start = 0;
    for (i, ch) in text.char_indices() {
        match ch {
            '{' => {
                if depth == 1 { block_start = i; } // stream-level object
                depth += 1;
            }
            '}' => {
                depth -= 1;
                if depth == 1 {
                    let block = &text[block_start..=i];
                    // Only care about subtitle streams
                    if !block.contains("\"codec_type\"") || !block.contains("\"subtitle\"") {
                        continue;
                    }
                    // Extract index
                    let idx = extract_json_int(block, "\"index\"");
                    let Some(idx) = idx else { continue };

                    let lang = extract_json_str(block, "\"language\"");
                    let title = extract_json_str(block, "\"title\"");

                    eprintln!("[NVENC] subtitle stream: index={idx} lang={lang:?} title={title:?}");

                    if first_sub.is_none() {
                        first_sub = Some(idx);
                    }
                    if lang.as_deref() == Some("eng") || lang.as_deref() == Some("English") {
                        if english_any.is_none() {
                            english_any = Some(idx);
                        }
                        // Prefer "full" subs over signs/songs
                        let t = title.as_deref().unwrap_or("").to_lowercase();
                        if !t.contains("sign") && !t.contains("song") && !t.contains("forced") {
                            if english_full.is_none() {
                                english_full = Some(idx);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Priority: English full > English any > first sub
    let result = english_full.or(english_any).or(first_sub);
    if let Some(idx) = result {
        eprintln!("[NVENC] selected subtitle stream index {idx}");
    } else {
        eprintln!("[NVENC] no subtitle streams found");
    }
    result
}

/// Extract an integer value from a JSON key like `"index": 4`
fn extract_json_int(block: &str, key: &str) -> Option<i32> {
    let pos = block.find(key)?;
    let after = &block[pos + key.len()..];
    let num_start = after.find(|c: char| c.is_ascii_digit())?;
    let num_str: String = after[num_start..].chars().take_while(|c| c.is_ascii_digit()).collect();
    num_str.parse().ok()
}

/// Extract a string value from a JSON key like `"language": "eng"`
fn extract_json_str(block: &str, key: &str) -> Option<String> {
    let pos = block.find(key)?;
    let after = &block[pos + key.len()..];
    let quote_start = after.find('"')? + 1;
    let quote_end = after[quote_start..].find('"')?;
    Some(after[quote_start..quote_start + quote_end].to_string())
}

/// Spawn an ffmpeg process that transcodes the input file to HLS with NVENC,
/// optionally burning in subtitles via libass.
async fn start_nvenc_transcode(
    ffmpeg: &str,
    file_path: &str,
    sub_index: Option<i32>,
    output_dir: &Path,
) -> Result<tokio::process::Child, String> {
    let m3u8 = output_dir.join("stream.m3u8");
    let segment_pattern = output_dir.join("seg%05d.ts");

    // Extract subtitles to a temp file so libass can read them without path-escaping nightmares
    let sub_file = if let Some(si) = sub_index {
        let sub_path = output_dir.join("subs.ass");
        eprintln!("[NVENC] extracting subtitles (stream index {si}) to {}", sub_path.display());
        let status = tokio::process::Command::new(ffmpeg)
            .arg("-i").arg(file_path)
            .arg("-map").arg(format!("0:{si}"))
            .arg("-c:s").arg("copy")
            .arg("-y")
            .arg(sub_path.to_str().unwrap())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;
        match status {
            Ok(s) if s.success() && sub_path.exists() => {
                eprintln!("[NVENC] subtitle extraction OK");
                Some(sub_path)
            }
            _ => {
                eprintln!("[NVENC] subtitle extraction failed, continuing without subs");
                None
            }
        }
    } else {
        None
    };

    let mut cmd = tokio::process::Command::new(ffmpeg);
    cmd.arg("-i").arg(file_path);

    if let Some(ref sub_path) = sub_file {
        // libass subtitle filter needs forward slashes and escaped colons
        let sub_str = sub_path.to_str().unwrap().replace('\\', "/").replace(":", "\\:");
        cmd.arg("-vf").arg(format!("subtitles='{}',format=yuv420p", sub_str));
    } else {
        // Still need format=yuv420p for 10-bit sources (AV1, HEVC Main 10)
        cmd.arg("-vf").arg("format=yuv420p");
    }

    cmd.arg("-map").arg("0:v:0")
        .arg("-map").arg("0:a:0")
        .arg("-c:v").arg("h264_nvenc")
        .arg("-preset").arg("slow")
        .arg("-cq").arg("22")
        // Transcode audio to AAC for maximum compatibility (some sources have
        // codecs the TV can't handle in HLS/MPEGTS like FLAC, Opus, etc.)
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

    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());

    eprintln!("[NVENC] starting ffmpeg with NVENC...");
    let mut child = cmd.spawn().map_err(|e| format!("spawn failed: {e}"))?;

    // Stream ffmpeg stderr to proxy console so we can see progress/errors
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt;
            let reader = tokio::io::BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("[ffmpeg] {line}");
            }
        });
    }

    Ok(child)
}

/// Serve an HLS manifest, rewriting local segment filenames to session-relative URLs
/// that the TV will fetch back through the proxy.
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

            let body = Full::new(Bytes::from(rewritten))
                .map_err(|_| unreachable!())
                .boxed();
            Ok(Response::builder()
                .status(200)
                .header("content-type", "application/vnd.apple.mpegurl")
                .header("access-control-allow-origin", "*")
                .body(body)
                .unwrap())
        }
        Err(_) => {
            let body = Full::new(Bytes::from("manifest not ready"))
                .map_err(|_| unreachable!())
                .boxed();
            Ok(Response::builder()
                .status(503)
                .body(body)
                .unwrap())
        }
    }
}

/// Serve a segment file (or m3u8 sub-manifest) from disk, waiting up to 10 seconds
/// for it to appear (ffmpeg may still be encoding it).
async fn serve_file(
    path: &Path,
    name: &str,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    for _ in 0..40 {
        if path.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    match tokio::fs::read(path).await {
        Ok(data) => {
            let content_type = if name.ends_with(".m3u8") {
                "application/vnd.apple.mpegurl"
            } else {
                "video/mp2t"
            };
            let body = Full::new(Bytes::from(data))
                .map_err(|_| unreachable!())
                .boxed();
            Ok(Response::builder()
                .status(200)
                .header("content-type", content_type)
                .header("access-control-allow-origin", "*")
                .body(body)
                .unwrap())
        }
        Err(_) => {
            let body = Full::new(Bytes::from("segment not found"))
                .map_err(|_| unreachable!())
                .boxed();
            Ok(Response::builder()
                .status(404)
                .body(body)
                .unwrap())
        }
    }
}

/// Forward a request to the real Plex backend, streaming the response back to the client.
async fn proxy_to_plex(
    client: reqwest::Client,
    plex_backend: &str,
    req: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, reqwest::Error>>, hyper::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path_and_query = uri.path_and_query().map(|p| p.as_str()).unwrap_or("/");
    let url = format!("{plex_backend}{path_and_query}");

    let mut builder = client.request(method, &url);
    for (name, value) in req.headers() {
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
            eprintln!("backend error: {e}");
            let body = Full::new(Bytes::from("bad gateway"))
                .map_err(|_| unreachable!())
                .boxed();
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(body)
                .unwrap());
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
