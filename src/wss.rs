use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    path::PathBuf,
};

use crate::{tester_oracle, Cli};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

pub(crate) fn handle_connection(mut stream: TcpStream, request: &str, server_cli: &Cli) {
    if let Err(e) = handle_connection_result(&mut stream, request, server_cli) {
        let _ = send_text(
            &mut stream,
            &format!(
                "{{\"type\":\"error\",\"ok\":false,\"error\":\"{}\"}}",
                json_escape(&e.to_string())
            ),
        );
        let _ = send_close(&mut stream);
    }
}

fn handle_connection_result(
    stream: &mut TcpStream,
    request: &str,
    server_cli: &Cli,
) -> io::Result<()> {
    let key = websocket_key(request)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing websocket key"))?;
    let accept = websocket_accept(&key);
    write!(
        stream,
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\
         \r\n"
    )?;

    send_text(
        stream,
        "{\"type\":\"ready\",\"ok\":true,\"message\":\"send URL-encoded run options\"}",
    )?;

    let command = match read_text(stream)? {
        WsMessage::Text(s) => s,
        WsMessage::Close => return Ok(()),
    };
    let params = parse_params(&command)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let cli = oracle_cli_from_params(server_cli, &params)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    let root = cli
        .fname
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let output = cli
        .oracle_output
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| format!("{root}/oracle-report.bin"));
    let chain_length = cli.numops.unwrap_or(2);

    send_text(
        stream,
        &format!(
            "{{\"type\":\"started\",\"ok\":true,\"root\":\"{}\",\"output\":\"{}\",\"chain_length\":{}}}",
            json_escape(&root),
            json_escape(&output),
            chain_length
        ),
    )?;

    let run_result = tester_oracle::run_with_events(cli, |event| {
        match event {
        tester_oracle::OracleEvent::Progress(progress) => send_text(
            stream,
            &format!(
                "{{\"type\":\"progress\",\"ok\":true,\"timestamp\":\"{}\",\"percent\":{:.2},\"index\":{},\"total\":{},\"line\":\"{}\"}}",
                json_escape(&progress.timestamp),
                progress.percent,
                progress.index,
                progress.total,
                json_escape(&progress.line)
            ),
        )
        .map_err(|e| e.to_string()),
        tester_oracle::OracleEvent::Status(status) => send_text(
            stream,
            &format!(
                "{{\"type\":\"{}\",\"ok\":true,\"message\":\"{}\",\"expected\":{},\"actual\":{}}}",
                status.phase,
                json_escape(&status.message),
                json_string(status.expected.as_deref()),
                json_string(status.actual.as_deref())
            ),
        )
        .map_err(|e| e.to_string()),
    }
    });

    match run_result {
        Ok(()) => send_text(stream, "{\"type\":\"done\",\"ok\":true}")?,
        Err(e) => send_text(
            stream,
            &format!(
                "{{\"type\":\"error\",\"ok\":false,\"error\":\"{}\"}}",
                json_escape(&e)
            ),
        )?,
    }
    send_close(stream)
}

fn oracle_cli_from_params(
    server_cli: &Cli,
    params: &HashMap<String, String>,
) -> Result<Cli, String> {
    let root = params
        .get("root")
        .or_else(|| params.get("cwd"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data/fsx-oracle-ws"));

    let mut cli = server_cli.clone();
    cli.server = None;
    cli.oracle = true;
    cli.orchestrated = false;
    cli.oracle_verify_files = None;
    cli.fname = Some(root.clone());
    cli.numops = Some(
        param_parse::<u64>(params, "chain_length")?
            .or(param_parse::<u64>(params, "n")?)
            .or(param_parse::<u64>(params, "numops")?)
            .unwrap_or(2),
    );
    if cli.numops == Some(0) {
        return Err("chain length must be greater than zero".to_owned());
    }
    cli.oracle_output = params
        .get("oracle_output")
        .or_else(|| params.get("output"))
        .map(PathBuf::from)
        .or_else(|| Some(root.join("oracle-report.bin")));
    cli.oracle_expected = params
        .get("oracle_expected")
        .or_else(|| params.get("expected"))
        .filter(|v| !v.trim().is_empty())
        .map(PathBuf::from);
    Ok(cli)
}

enum WsMessage {
    Text(String),
    Close,
}

fn read_text(stream: &mut TcpStream) -> io::Result<WsMessage> {
    loop {
        let mut header = [0u8; 2];
        stream.read_exact(&mut header)?;
        let opcode = header[0] & 0x0f;
        let masked = (header[1] & 0x80) != 0;
        let mut len = u64::from(header[1] & 0x7f);
        if len == 126 {
            let mut ext = [0u8; 2];
            stream.read_exact(&mut ext)?;
            len = u64::from(u16::from_be_bytes(ext));
        } else if len == 127 {
            let mut ext = [0u8; 8];
            stream.read_exact(&mut ext)?;
            len = u64::from_be_bytes(ext);
        }
        if len > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "websocket message is too large",
            ));
        }

        let mut mask = [0u8; 4];
        if masked {
            stream.read_exact(&mut mask)?;
        }
        let mut payload = vec![0u8; len as usize];
        stream.read_exact(&mut payload)?;
        if masked {
            for (idx, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[idx % 4];
            }
        }

        match opcode {
            0x1 => {
                let text = String::from_utf8(payload).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "text frame is not utf-8")
                })?;
                return Ok(WsMessage::Text(text));
            }
            0x8 => return Ok(WsMessage::Close),
            0x9 => send_frame(stream, 0xA, &payload)?,
            _ => {}
        }
    }
}

fn send_text(stream: &mut TcpStream, text: &str) -> io::Result<()> {
    send_frame(stream, 0x1, text.as_bytes())
}

fn send_close(stream: &mut TcpStream) -> io::Result<()> {
    send_frame(stream, 0x8, &[])
}

fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> io::Result<()> {
    let mut header = Vec::with_capacity(10);
    header.push(0x80 | opcode);
    if payload.len() < 126 {
        header.push(payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        header.push(127);
        header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    stream.write_all(&header)?;
    stream.write_all(payload)?;
    stream.flush()
}

fn websocket_key(request: &str) -> Option<String> {
    request.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("Sec-WebSocket-Key") {
            Some(value.trim().to_owned())
        } else {
            None
        }
    })
}

fn websocket_accept(key: &str) -> String {
    let mut data = Vec::with_capacity(key.len() + WS_GUID.len());
    data.extend_from_slice(key.as_bytes());
    data.extend_from_slice(WS_GUID.as_bytes());
    base64_encode(&sha1(&data))
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
    String::from_utf8(out).map_err(|_| "query value is not valid utf-8".to_owned())
}

fn parse_params(s: &str) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    for part in s.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        params.insert(decode_url_component(k)?, decode_url_component(v)?);
    }
    Ok(params)
}

fn param_parse<T>(params: &HashMap<String, String>, name: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    params
        .get(name)
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.parse::<T>().map_err(|e| format!("invalid {name}: {e}")))
        .transpose()
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
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn json_string(value: Option<&str>) -> String {
    match value {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_owned(),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn sha1(input: &[u8]) -> [u8; 20] {
    let mut h0 = 0x67452301u32;
    let mut h1 = 0xefcdab89u32;
    let mut h2 = 0x98badcfeu32;
    let mut h3 = 0x10325476u32;
    let mut h4 = 0xc3d2e1f0u32;

    let bit_len = (input.len() as u64) * 8;
    let mut data = input.to_vec();
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in data.chunks_exact(64) {
        let mut w = [0u32; 80];
        for (idx, word) in w.iter_mut().take(16).enumerate() {
            let start = idx * 4;
            *word = u32::from_be_bytes([
                chunk[start],
                chunk[start + 1],
                chunk[start + 2],
                chunk[start + 3],
            ]);
        }
        for idx in 16..80 {
            w[idx] = (w[idx - 3] ^ w[idx - 8] ^ w[idx - 14] ^ w[idx - 16]).rotate_left(1);
        }

        let mut a = h0;
        let mut b = h1;
        let mut c = h2;
        let mut d = h3;
        let mut e = h4;
        for (idx, word) in w.iter().enumerate() {
            let (f, k) = match idx {
                0..=19 => ((b & c) | ((!b) & d), 0x5a827999),
                20..=39 => (b ^ c ^ d, 0x6ed9eba1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1bbcdc),
                _ => (b ^ c ^ d, 0xca62c1d6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}
