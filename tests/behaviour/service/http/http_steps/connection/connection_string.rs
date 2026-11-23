/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::str::FromStr;

use hyper::http::Uri;

use crate::HttpBehaviourTestError;

const CONNECTION_STRING_SCHEME: &str = "typedb://";
const DEFAULT_PORT: u16 = 8000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Addresses {
    Direct(Vec<String>),
    Translated(Vec<(String, String)>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConnectionString {
    addresses: Addresses,
    username: String,
    password: String,
}

impl ConnectionString {
    pub(crate) fn addresses(&self) -> &Addresses {
        &self.addresses
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }
}

impl FromStr for ConnectionString {
    type Err = HttpBehaviourTestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let rest = value
            .strip_prefix(CONNECTION_STRING_SCHEME)
            .ok_or_else(|| invalid(format!("expected it to start with '{CONNECTION_STRING_SCHEME}'")))?;
        let rest = rest.split_once('?').map_or(rest, |(rest, _query)| rest);
        // A password may contain an unencoded '@', so the last one separates the addresses.
        let (credentials, addresses) = rest
            .rsplit_once('@')
            .ok_or_else(|| invalid("expected credentials, separated from the addresses by '@'"))?;
        let (username, password) = credentials
            .split_once(':')
            .filter(|(username, password)| !username.is_empty() && !password.is_empty())
            .ok_or_else(|| invalid("expected a non-empty username and password, separated by ':'"))?;

        let mut tls = Vec::new();
        let mut parse = |address: &str| {
            let (address, is_tls_enabled) = parse_address(address)?;
            tls.push(is_tls_enabled);
            Ok(address)
        };
        let pairs: Vec<_> = addresses.split(',').map(|entry| entry.split_once(';')).collect();
        let addresses = if pairs.iter().all(Option::is_none) {
            Addresses::Direct(addresses.split(',').map(&mut parse).collect::<Result<Vec<_>, _>>()?)
        } else if let Some(pairs) = pairs.into_iter().collect::<Option<Vec<_>>>() {
            let translation = pairs.into_iter().map(|(public, private)| Ok((parse(public)?, parse(private)?)));
            Addresses::Translated(translation.collect::<Result<Vec<_>, _>>()?)
        } else {
            return Err(invalid("expected either all addresses or none to be 'public;private' translation pairs"));
        };

        if tls.iter().any(|is_tls_enabled| *is_tls_enabled != tls[0]) {
            return Err(invalid(
                "expected all addresses to use the same scheme, as one TLS configuration applies to the whole connection",
            ));
        }
        Ok(Self {
            addresses,
            username: percent_decode(username).ok_or_else(|| invalid("expected a percent-encoded UTF-8 username"))?,
            password: percent_decode(password).ok_or_else(|| invalid("expected a percent-encoded UTF-8 password"))?,
        })
    }
}

fn parse_address(address: &str) -> Result<(String, bool), HttpBehaviourTestError> {
    let malformed = || invalid(format!("expected '{address}' to be a valid address"));
    let uri = address.parse::<Uri>().map_err(|_| malformed())?;
    let (Some(scheme @ ("http" | "https")), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
        return Err(invalid(format!(
            "expected address '{address}' to specify either the 'http://' or the 'https://' scheme"
        )));
    };
    let host = authority.host();
    let port = match authority.port_u16() {
        _ if host.is_empty() => return Err(malformed()),
        Some(port) => port,
        None if authority.as_str() == host => DEFAULT_PORT,
        None => return Err(malformed()),
    };
    Ok((format!("{scheme}://{host}:{port}"), scheme == "https"))
}

fn percent_decode(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    let mut decoded = Vec::with_capacity(value.len());
    while let Some(byte) = bytes.next() {
        decoded.push(match byte {
            b'%' => {
                let (high, low) = (char::from(bytes.next()?).to_digit(16)?, char::from(bytes.next()?).to_digit(16)?);
                (high * 16 + low) as u8
            }
            byte => byte,
        });
    }
    String::from_utf8(decoded).ok()
}

fn invalid(reason: impl Into<String>) -> HttpBehaviourTestError {
    HttpBehaviourTestError::InvalidConnectionString { reason: reason.into() }
}
