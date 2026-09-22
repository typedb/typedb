/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

const DISABLED: &str = "disabled";
const UNKNOWN: &str = "<UNKNOWN ADDRESS>";

#[cfg(unix)]
const ADMIN_TRANSPORT_LABEL: &str = "Unix socket";
#[cfg(windows)]
const ADMIN_TRANSPORT_LABEL: &str = "Named Pipe";

#[derive(Clone, Debug, Default)]
pub struct EndpointInfo {
    pub listen: Option<String>,
    pub advertise: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ServingInfo {
    pub grpc: EndpointInfo,
    pub http: Option<EndpointInfo>,
    pub admin: Option<String>,
    pub monitoring: Option<String>,
}

impl fmt::Display for ServingInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Serving:")?;
        writeln!(f, "  gRPC:       {}", endpoint_display(&self.grpc))?;
        match &self.http {
            Some(http) => writeln!(f, "  HTTP:       {}", endpoint_display(http))?,
            None => writeln!(f, "  HTTP:       {DISABLED}")?,
        }
        match &self.admin {
            Some(admin) => writeln!(f, "  Admin:      {admin} ({ADMIN_TRANSPORT_LABEL})")?,
            None => writeln!(f, "  Admin:      {DISABLED}")?,
        }
        match &self.monitoring {
            Some(monitoring) => {
                writeln!(f, "  Monitoring: http://{monitoring}/diagnostics (Prometheus scrape)")?;
                write!(f, "              http://{monitoring}/diagnostics?format=json (JSON)")
            }
            None => write!(f, "  Monitoring: {DISABLED}"),
        }
    }
}

fn endpoint_display(endpoint: &EndpointInfo) -> String {
    let listen = endpoint.listen.as_deref().filter(|s| !s.is_empty()).unwrap_or(UNKNOWN);
    match endpoint.advertise.as_deref() {
        Some(advertise) if advertise != listen => format!("{listen} (connect via {advertise})"),
        _ => listen.to_string(),
    }
}
