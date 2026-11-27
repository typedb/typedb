/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Error reporting: panics, and errors reported explicitly through `error_with_report!`.
//! Reports go to the TypeDB diagnostics relay (typedb-web, netlify/edge-functions/diagnostics-report), which maps
//! this provider-neutral payload onto whichever error tracker is current. The payload schema is documented there.

use std::{
    backtrace::Backtrace,
    env,
    panic::PanicHookInfo,
    path::Path,
    sync::OnceLock,
    thread::{self, JoinHandle},
};

use hyper::{Body, Client, Request, header::CONTENT_TYPE};
use hyper_rustls::HttpsConnectorBuilder;
use resource::constants::diagnostics::{ERROR_REPORTING_TIMEOUT, ERROR_REPORTING_URI, UNKNOWN_STR};
use serde_json::{Value as JSONValue, json};

const ENV_URL: &str = "TYPEDB_SERVER_DIAGNOSTICS_URL";
const ENV_DEBUG: &str = "TYPEDB_SERVER_DIAGNOSTICS_DEBUG";
const ENV_TEST_PANIC: &str = "TYPEDB_SERVER_DIAGNOSTICS_TEST_PANIC";

static CONFIG: OnceLock<Config> = OnceLock::new();
static INSTALL_ID: OnceLock<String> = OnceLock::new();

struct Config {
    url: String,
    product: &'static str,
    version: &'static str,
    debug: bool,
}

#[derive(Debug, Copy, Clone)]
pub enum Level {
    Error,
    Warning,
}

impl Level {
    fn kind(self) -> &'static str {
        match self {
            Level::Error => "Error",
            Level::Warning => "Warning",
        }
    }
}

pub fn init(product: &'static str, version: &'static str) {
    let url = env::var(ENV_URL).unwrap_or_else(|_| ERROR_REPORTING_URI.to_owned());
    let debug = env::var(ENV_DEBUG).is_ok();
    let _ = CONFIG.set(Config { url, product, version: version.trim(), debug });
    if env::var(ENV_TEST_PANIC).is_ok() {
        panic!("diagnostics test panic");
    }
}

pub fn set_install_id(install_id: &str) {
    let _ = INSTALL_ID.set(install_id.to_owned());
}

pub fn report_panic(info: &PanicHookInfo<'_>) {
    let Some(config) = CONFIG.get() else { return };
    let backtrace = Backtrace::force_capture().to_string();
    let error = json!({
        "kind": "Panic",
        "message": panic_message(info),
        "lang": "rust",
        "frames": parse_frames(&backtrace),
        "backtrace": backtrace,
    });
    let _ = send(config, error).join();
}

pub fn report_message(message: &str, level: Level) {
    let Some(config) = CONFIG.get() else { return };
    send(config, json!({ "kind": level.kind(), "message": message, "lang": "rust" }));
}

fn send(config: &'static Config, error: JSONValue) -> JoinHandle<()> {
    let payload = json!({
        "product": config.product,
        "version": config.version,
        "install_id": INSTALL_ID.get().map(String::as_str).unwrap_or(UNKNOWN_STR),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
        "error": error,
    });
    thread::spawn(move || {
        if config.debug {
            eprintln!("[diagnostics] sending to {}:\n{:#}", config.url, payload);
        }
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| err.to_string())
            .and_then(|runtime| runtime.block_on(post(&config.url, payload.to_string())));
        if config.debug {
            match result {
                Ok(status) => eprintln!("[diagnostics] sent, status {status}"),
                Err(err) => eprintln!("[diagnostics] send failed: {err}"),
            }
        }
    })
}

async fn post(url: &str, body: String) -> Result<u16, String> {
    let https = HttpsConnectorBuilder::new()
        .with_native_roots()
        .map_err(|err| err.to_string())?
        .https_or_http()
        .enable_http1()
        .build();
    let request = Request::post(url)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .map_err(|err| err.to_string())?;
    let response = Client::builder().build::<_, Body>(https).request(request);
    match tokio::time::timeout(ERROR_REPORTING_TIMEOUT, response).await {
        Ok(Ok(response)) => Ok(response.status().as_u16()),
        Ok(Err(err)) => Err(err.to_string()),
        Err(_) => Err("timed out".to_owned()),
    }
}

fn panic_message(info: &PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    let message = if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_owned()
    };
    match info.location() {
        Some(location) => format!("{message} (at {}:{})", location.file(), location.line()),
        None => message,
    }
}

fn parse_frames(backtrace: &str) -> Vec<JSONValue> {
    let mut frames: Vec<JSONValue> = Vec::new();
    for line in backtrace.lines() {
        let trimmed = line.trim_start();
        if let Some(rest) = trimmed.strip_prefix("at ") {
            if let Some(frame) = frames.last_mut() {
                let (filename, lineno) = split_location(rest);
                frame["file"] = json!(filename);
                frame["in_app"] = json!(is_in_app(filename));
                if let Some(lineno) = lineno {
                    frame["line"] = json!(lineno);
                }
            }
        } else if let Some((_, function)) = trimmed.split_once(": ") {
            frames.push(json!({ "function": function, "in_app": false }));
        }
    }
    trim_to_short_backtrace(frames)
}

fn is_in_app(filename: &str) -> bool {
    Path::new(filename).is_relative() && !filename.starts_with('/') && !filename.starts_with("external")
}

// Snip common frames so unrelated errors don't get grouped together in the error tracker
fn trim_to_short_backtrace(frames: Vec<JSONValue>) -> Vec<JSONValue> {
    let function = |frame: &JSONValue| frame["function"].as_str().unwrap_or("").to_owned();
    let Some(start) = frames.iter().position(|f| function(f).contains("__rust_end_short_backtrace")) else {
        return frames;
    };
    let end = frames.iter().position(|f| function(f).contains("__rust_begin_short_backtrace")).unwrap_or(frames.len());
    frames[start + 1..end.max(start + 1)]
        .iter()
        .skip_while(|f| {
            let name = function(f);
            name.starts_with("std::panicking::")
                || name.starts_with("core::panicking::")
                || name.contains("rust_begin_unwind")
        })
        .cloned()
        .collect()
}

fn split_location(location: &str) -> (&str, Option<u64>) {
    let is_number = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
    let mut file = location;
    let mut line = None;
    for _ in 0..2 {
        match file.rsplit_once(':') {
            Some((rest, number)) if is_number(number) => {
                line = number.parse().ok();
                file = rest;
            }
            _ => break,
        }
    }
    (file, line)
}
