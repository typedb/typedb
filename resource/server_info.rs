/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

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

pub fn print_serving_block(info: &ServingInfo) {
    println!("Serving:");
    println!("  gRPC:       {}", endpoint_display(&info.grpc));
    match &info.http {
        Some(http) => println!("  HTTP:       {}", endpoint_display(http)),
        None => println!("  HTTP:       {DISABLED}"),
    }
    match &info.admin {
        Some(admin) => println!("  Admin:      {admin} ({ADMIN_TRANSPORT_LABEL})"),
        None => println!("  Admin:      {DISABLED}"),
    }
    match &info.monitoring {
        Some(monitoring) => {
            println!("  Monitoring: http://{monitoring}/diagnostics (Prometheus scrape)");
            println!("              http://{monitoring}/diagnostics?format=json (JSON)");
        }
        None => println!("  Monitoring: {DISABLED}"),
    }
}

fn endpoint_display(endpoint: &EndpointInfo) -> String {
    let listen = endpoint.listen.as_deref().filter(|s| !s.is_empty()).unwrap_or(UNKNOWN);
    match endpoint.advertise.as_deref() {
        Some(advertise) if advertise != listen => format!("{listen} (connect via {advertise})"),
        _ => listen.to_string(),
    }
}
