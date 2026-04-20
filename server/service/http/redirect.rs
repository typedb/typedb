/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use axum::{body::Body, extract::Request, middleware::Next, response::Response};
use http::{header, StatusCode};

/// Middleware that appends the original request path to redirect Location headers.
/// When a handler returns a 307 redirect with a Location that has no path (e.g., `http://host:port`),
/// this middleware appends the original request's path and query string.
pub(crate) async fn append_request_path_to_redirect(request: Request<Body>, next: Next) -> Response {
    let original_uri = request.uri().clone();
    let mut response = next.run(request).await;

    if response.status() == StatusCode::TEMPORARY_REDIRECT {
        if let Some(location) = response.headers().get(header::LOCATION) {
            if let Ok(location_str) = location.to_str() {
                if let Ok(location_uri) = location_str.parse::<http::Uri>() {
                    if location_uri.path() == "/" || location_uri.path().is_empty() {
                        let path_and_query = original_uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
                        let new_location = format!("{}{}", location_str.trim_end_matches('/'), path_and_query);
                        if let Ok(value) = http::HeaderValue::from_str(&new_location) {
                            response.headers_mut().insert(header::LOCATION, value);
                        }
                    }
                }
            }
        }
    }

    response
}
