//! Root-only JSON API for users: `/_kosha/admin/users[/<access key>]`.
//!
//! * `GET users` lists users (secrets are never returned after creation).
//! * `POST users` `{"name", "access_key"?, "secret_key"?, "policy"?}`
//!   creates one and returns its keys.
//! * `GET users/<key>`, `DELETE users/<key>`.
//! * `PUT users/<key>` `{"policy"?, "enabled"?, "name"?, "rotate_secret"?}`
//!   updates one; `"policy": null` removes the policy.

use axum::body::Body;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use kosha_engine::User;
use serde_json::{Value, json};

use crate::AppState;
use crate::auth::Payload;
use crate::body::BodySource;
use crate::error::{S3Error, S3Result};
use crate::iam::{Policy, PolicyKind};

const MAX_BODY: usize = 64 * 1024;

fn public(u: &User) -> Value {
    json!({
        "access_key": u.access_key,
        "name": u.name,
        "policy": u.policy.as_deref().and_then(|p| serde_json::from_str::<Value>(p).ok()),
        "enabled": u.enabled,
        "created": u.created.to_rfc3339(),
    })
}

/// Random characters from `alphabet`, from the OS RNG via UUIDv4.
fn random(alphabet: &[u8], len: usize) -> String {
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for b in uuid::Uuid::new_v4().as_bytes() {
            // Rejection sampling keeps the distribution uniform.
            let limit = 256 - 256 % alphabet.len();
            if (*b as usize) < limit && out.len() < len {
                out.push(alphabet[*b as usize % alphabet.len()] as char);
            }
        }
    }
    out
}

fn new_access_key() -> String {
    format!("KS{}", random(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", 18))
}

fn new_secret() -> String {
    random(
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
        40,
    )
}

/// A policy given as a JSON object or as a string holding JSON; validated.
fn policy_from(v: &Value) -> S3Result<Option<String>> {
    let doc = match v {
        Value::Null => return Ok(None),
        Value::String(s) => s.clone(),
        v @ Value::Object(_) => v.to_string(),
        _ => return Err(S3Error::invalid_argument("policy must be a JSON object")),
    };
    Policy::parse(&doc, PolicyKind::Identity)
        .map_err(|e| S3Error::new(StatusCode::BAD_REQUEST, "MalformedPolicyDocument", e))?;
    Ok(Some(doc))
}

pub(crate) async fn read_json(
    body: Body,
    payload: Payload,
    headers: &HeaderMap,
) -> S3Result<Value> {
    let raw = BodySource::new(body, payload, headers)?
        .collect(MAX_BODY)
        .await?;
    if raw.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&raw)
        .map_err(|e| S3Error::invalid_argument(format!("body is not valid JSON: {e}")))
}

pub(crate) fn str_field<'a>(v: &'a Value, name: &str) -> S3Result<Option<&'a str>> {
    match v.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(S3Error::invalid_argument(format!(
            "{name} must be a string"
        ))),
    }
}

pub(crate) async fn handle(
    st: &AppState,
    method: &Method,
    path: &str,
    body: Body,
    payload: Payload,
    headers: &HeaderMap,
) -> S3Result<Response> {
    let engine = &st.engine;
    let path = path.trim_end_matches('/');
    let Some(rest) = path.strip_prefix("users") else {
        return Err(S3Error::new(
            StatusCode::NOT_FOUND,
            "NoSuchResource",
            "unknown admin endpoint",
        ));
    };
    let key = rest.strip_prefix('/').filter(|k| !k.is_empty());
    match (method.clone(), key) {
        (Method::GET, None) => {
            let users: Vec<Value> = engine.list_users().await?.iter().map(public).collect();
            Ok(axum::Json(json!({ "users": users })).into_response())
        }
        (Method::POST, None) => {
            let req = read_json(body, payload, headers).await?;
            let name = str_field(&req, "name")?
                .ok_or_else(|| S3Error::invalid_argument("name is required"))?;
            let access_key = str_field(&req, "access_key")?
                .map(str::to_string)
                .unwrap_or_else(new_access_key);
            if st.cfg.credentials.is_root(&access_key) {
                return Err(S3Error::new(
                    StatusCode::CONFLICT,
                    "EntityAlreadyExists",
                    "that access key is a root key",
                ));
            }
            let user = User {
                access_key,
                secret_key: str_field(&req, "secret_key")?
                    .map(str::to_string)
                    .unwrap_or_else(new_secret),
                name: name.to_string(),
                policy: policy_from(req.get("policy").unwrap_or(&Value::Null))?,
                enabled: req.get("enabled").and_then(Value::as_bool).unwrap_or(true),
                created: Utc::now(),
            };
            engine.create_user(user.clone()).await?;
            let mut out = public(&user);
            out["secret_key"] = json!(user.secret_key);
            Ok((StatusCode::CREATED, axum::Json(out)).into_response())
        }
        (Method::GET, Some(k)) => Ok(axum::Json(public(&engine.user(k).await?)).into_response()),
        (Method::PUT, Some(k)) => {
            let req = read_json(body, payload, headers).await?;
            let mut user = engine.user(k).await?;
            if let Some(p) = req.get("policy") {
                user.policy = policy_from(p)?;
            }
            if let Some(e) = req.get("enabled") {
                user.enabled = e
                    .as_bool()
                    .ok_or_else(|| S3Error::invalid_argument("enabled must be a boolean"))?;
            }
            if let Some(n) = str_field(&req, "name")? {
                user.name = n.to_string();
            }
            let rotate = req
                .get("rotate_secret")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if rotate {
                user.secret_key = new_secret();
            }
            engine.update_user(user.clone()).await?;
            let mut out = public(&user);
            if rotate {
                out["secret_key"] = json!(user.secret_key);
            }
            Ok(axum::Json(out).into_response())
        }
        (Method::DELETE, Some(k)) => {
            engine.delete_user(k).await?;
            Ok(StatusCode::NO_CONTENT.into_response())
        }
        _ => Err(S3Error::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "MethodNotAllowed",
            "method not allowed on this admin endpoint",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_keys_have_the_right_shape() {
        let a = new_access_key();
        assert_eq!(a.len(), 20);
        assert!(a.starts_with("KS"));
        let s = new_secret();
        assert_eq!(s.len(), 40);
        assert_ne!(new_secret(), s);
    }
}
