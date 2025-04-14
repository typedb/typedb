/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use axum::{
    async_trait,
    extract::{FromRequest, Request},
    response::{IntoResponse, Response},
    Json,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::service::http::error::HttpServiceError;

pub(crate) struct JsonBody<T>(pub T);

impl<T> From<Json<T>> for JsonBody<T> {
    fn from(Json(value): Json<T>) -> Self {
        Self(value)
    }
}

impl<T> Into<Json<T>> for JsonBody<T> {
    fn into(self) -> Json<T> {
        Json(self.0)
    }
}

#[async_trait]
impl<T, S> FromRequest<S> for JsonBody<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = HttpServiceError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::from_request(req, state)
            .await
            .map(|json: Json<T>| json.into())
            .map_err(|err| Self::Rejection::JsonBodyExpected { details: err.body_text() })
    }
}

impl<T: Serialize> IntoResponse for JsonBody<T> {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

pub(crate) struct PlainTextBody(pub String);

impl IntoResponse for PlainTextBody {
    fn into_response(self) -> Response {
        self.0.into_response()
    }
}
