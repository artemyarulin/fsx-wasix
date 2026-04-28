use std::{
    collections::HashMap,
    fmt, fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use crate::{
    tester_fsx::{run_workers, worker_count, Config},
    Cli,
};

fn server_port(cli: &Cli) -> u16 {
    cli.server
        .flatten()
        .or_else(|| {
            std::env::var("PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
        })
        .unwrap_or(8080)
}

fn decode_url_component(s: &str) -> Result<String, String> {
    let mut out = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .map_err(|_| "invalid percent encoding".to_owned())?;
                let b = u8::from_str_radix(hex, 16)
                    .map_err(|_| "invalid percent encoding".to_owned())?;
                out.push(b);
                i += 3;
            }
            b'%' => return Err("truncated percent encoding".to_owned()),
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| "query value is not valid utf-8".to_owned())
}

fn parse_params(s: &str) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    for part in s.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        params.insert(decode_url_component(k)?, decode_url_component(v)?);
    }
    Ok(params)
}

fn param_parse<T>(
    params: &HashMap<String, String>,
    name: &str,
) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    params
        .get(name)
        .map(|v| v.parse::<T>().map_err(|e| format!("invalid {name}: {e}")))
        .transpose()
}

fn param_bool(
    params: &HashMap<String, String>,
    name: &str,
) -> Result<Option<bool>, String> {
    params
        .get(name)
        .map(|v| match v.as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(format!("invalid {name}: expected boolean")),
        })
        .transpose()
}

fn apply_weight_param(
    params: &HashMap<String, String>,
    name: &str,
    weight: &mut f64,
) -> Result<(), String> {
    if let Some(v) = param_parse::<f64>(params, name)? {
        *weight = v;
    }
    Ok(())
}

fn server_run_from_params(
    server_cli: &Cli,
    base_config: &Config,
    params: &HashMap<String, String>,
) -> Result<(Cli, Config, Option<Duration>), String> {
    let cwd = params
        .get("cwd")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&cwd)
        .map_err(|e| format!("failed to create cwd: {e}"))?;

    let fname = if let Some(path) = params.get("path") {
        let p = PathBuf::from(path);
        if p.is_absolute() {
            p
        } else {
            cwd.join(p)
        }
    } else {
        cwd.join(params.get("file").map(String::as_str).unwrap_or("fsxfile"))
    };
    if let Some(parent) = fname.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create target directory: {e}"))?;
    }

    let mut cli = server_cli.clone();
    cli.fname = Some(fname);
    cli.config = None;
    cli.server = None;
    cli.inject = param_parse::<u64>(params, "inject")?;
    cli.numops = Some(
        param_parse::<u64>(params, "numops")?
            .or(param_parse::<u64>(params, "n")?)
            .unwrap_or(1000),
    );
    if let Some(v) = param_parse::<NonZeroU64>(params, "opnum")? {
        cli.opnum = v;
    }
    if let Some(v) = param_parse::<u64>(params, "seed")? {
        cli.seed = Some(v);
    }
    if let Some(v) = param_parse::<NonZeroUsize>(params, "threads")?
        .or(param_parse::<NonZeroUsize>(params, "j")?)
    {
        cli.threads = Some(v);
    }
    if let Some(v) = params.get("artifacts_dir") {
        cli.artifacts_dir = Some(PathBuf::from(v));
    }

    let mut config = base_config.clone();
    if let Some(v) = param_parse::<u32>(params, "flen")? {
        config.flen = Some(v);
    }
    if let Some(v) = param_bool(params, "nosizechecks")? {
        config.nosizechecks = v;
    }
    if let Some(v) = param_parse::<usize>(params, "opsize_min")? {
        config.opsize.min = v;
    }
    if let Some(v) = param_parse::<usize>(params, "opsize_max")? {
        config.opsize.max = v;
    }
    if let Some(v) = param_parse::<NonZeroUsize>(params, "opsize_align")? {
        config.opsize.align = Some(v);
    }

    apply_weight_param(params, "close_open", &mut config.weights.close_open)?;
    apply_weight_param(params, "read", &mut config.weights.read)?;
    apply_weight_param(params, "write", &mut config.weights.write)?;
    apply_weight_param(params, "mapread", &mut config.weights.mapread)?;
    apply_weight_param(params, "mapwrite", &mut config.weights.mapwrite)?;
    apply_weight_param(params, "truncate", &mut config.weights.truncate)?;
    apply_weight_param(params, "fsync", &mut config.weights.fsync)?;
    apply_weight_param(params, "fdatasync", &mut config.weights.fdatasync)?;

    config.validate_result(&cli)?;
    let timeout =
        param_parse::<u64>(params, "timeout_ms")?.map(Duration::from_millis);
    Ok((cli, config, timeout))
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04x}", c as u32))
            }
            c => out.push(c),
        }
    }
    out
}

fn http_response(status: &str, content_type: &str, body: String) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn json_response(status: &str, body: String) -> Vec<u8> {
    http_response(status, "application/json", body)
}

fn run_endpoint(
    server_cli: &Cli,
    base_config: &Config,
    params: HashMap<String, String>,
) -> Vec<u8> {
    let (cli, config, timeout) =
        match server_run_from_params(server_cli, base_config, &params) {
            Ok(v) => v,
            Err(e) => {
                return json_response(
                    "400 Bad Request",
                    format!(
                        "{{\"ok\":false,\"error\":\"{}\"}}\n",
                        json_escape(&e)
                    ),
                )
            }
        };

    let workers = worker_count(&cli);
    let target = cli
        .fname
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let numops = cli.numops.unwrap_or(0);
    let started = Instant::now();

    if let Some(timeout) = timeout {
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let r = run_workers(cli, config);
            let _ = tx.send(r);
        });
        match rx.recv_timeout(timeout) {
            Ok(Ok(summary)) => json_response(
                "200 OK",
                format!(
                    "{{\"ok\":true,\"workers\":{},\"numops_per_worker\":{},\"total_ops_planned\":{},\"elapsed_ms\":{},\"target\":\"{}\"}}\n",
                    summary.workers,
                    numops,
                    numops.saturating_mul(summary.workers as u64),
                    summary.elapsed.as_millis(),
                    json_escape(&target),
                ),
            ),
            Ok(Err(e)) => json_response(
                "500 Internal Server Error",
                format!(
                    "{{\"ok\":false,\"workers\":{},\"numops_per_worker\":{},\"elapsed_ms\":{},\"target\":\"{}\",\"error\":\"{}\"}}\n",
                    workers,
                    numops,
                    started.elapsed().as_millis(),
                    json_escape(&target),
                    json_escape(&e),
                ),
            ),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => json_response(
                "504 Gateway Timeout",
                format!(
                    "{{\"ok\":false,\"workers\":{},\"numops_per_worker\":{},\"elapsed_ms\":{},\"target\":\"{}\",\"error\":\"timeout exceeded; run continues in background\"}}\n",
                    workers,
                    numops,
                    started.elapsed().as_millis(),
                    json_escape(&target),
                ),
            ),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => json_response(
                "500 Internal Server Error",
                "{\"ok\":false,\"error\":\"worker coordinator disconnected\"}\n".to_owned(),
            ),
        }
    } else {
        match run_workers(cli, config) {
            Ok(summary) => json_response(
                "200 OK",
                format!(
                    "{{\"ok\":true,\"workers\":{},\"numops_per_worker\":{},\"total_ops_planned\":{},\"elapsed_ms\":{},\"target\":\"{}\"}}\n",
                    summary.workers,
                    numops,
                    numops.saturating_mul(summary.workers as u64),
                    summary.elapsed.as_millis(),
                    json_escape(&target),
                ),
            ),
            Err(e) => json_response(
                "500 Internal Server Error",
                format!(
                    "{{\"ok\":false,\"workers\":{},\"numops_per_worker\":{},\"elapsed_ms\":{},\"target\":\"{}\",\"error\":\"{}\"}}\n",
                    workers,
                    numops,
                    started.elapsed().as_millis(),
                    json_escape(&target),
                    json_escape(&e),
                ),
            ),
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    server_cli: &Cli,
    base_config: &Config,
) {
    let mut buf = vec![0u8; 16 * 1024];
    let n = match stream.read(&mut buf) {
        Ok(0) | Err(_) => return,
        Ok(n) => n,
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    let mut lines = request.lines();
    let Some(request_line) = lines.next() else {
        return;
    };
    let fields = request_line.split_ascii_whitespace().collect::<Vec<_>>();
    if fields.len() < 2 {
        let _ = stream.write_all(&json_response(
            "400 Bad Request",
            "{\"ok\":false,\"error\":\"bad request line\"}\n".to_owned(),
        ));
        return;
    }

    let method = fields[0];
    let target = fields[1];
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let body = request.split("\r\n\r\n").nth(1).unwrap_or("");
    let response = match (method, path) {
        ("GET", "/") => http_response(
            "200 OK",
            "text/plain; charset=utf-8",
            "fsx HTTP server\n\nGET /health\nGET /run?cwd=/data&file=fsxfile&n=1000&threads=4&seed=1&flen=10485760\nPOST /run with application/x-www-form-urlencoded body\n\nRun parameters: cwd, file, path, n/numops, threads/j, seed, opnum, timeout_ms, flen, nosizechecks, opsize_min, opsize_max, opsize_align, artifacts_dir, inject, and operation weights such as close_open/read/write/mapread/mapwrite/truncate/fsync/fdatasync.\n".to_owned(),
        ),
        ("GET", "/health") => json_response("200 OK", "{\"ok\":true}\n".to_owned()),
        ("GET", "/run") => match parse_params(query) {
            Ok(params) => run_endpoint(server_cli, base_config, params),
            Err(e) => json_response(
                "400 Bad Request",
                format!("{{\"ok\":false,\"error\":\"{}\"}}\n", json_escape(&e)),
            ),
        },
        ("POST", "/run") => match parse_params(body) {
            Ok(params) => run_endpoint(server_cli, base_config, params),
            Err(e) => json_response(
                "400 Bad Request",
                format!("{{\"ok\":false,\"error\":\"{}\"}}\n", json_escape(&e)),
            ),
        },
        _ => json_response(
            "404 Not Found",
            "{\"ok\":false,\"error\":\"not found\"}\n".to_owned(),
        ),
    };

    let _ = stream.write_all(&response);
}

pub(crate) fn run_server(cli: Cli, config: Config) -> io::Result<()> {
    let port = server_port(&cli);
    let listener = TcpListener::bind(("0.0.0.0", port))?;
    println!("Listening on http://0.0.0.0:{port}");
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_connection(stream, &cli, &config),
            Err(e) => eprintln!("error: failed to accept connection: {e}"),
        }
    }
    Ok(())
}
