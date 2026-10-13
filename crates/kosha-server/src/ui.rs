//! The web console: a React app compiled into the binary and served under
//! `/_kosha/ui/`. It talks to the regular S3 API (requests are SigV4-signed
//! in the browser), so it needs no extra server-side endpoints or sessions.

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../ui/dist"]
#[allow_missing = true]
struct Assets;

pub fn router() -> Router {
    Router::new()
        .route(
            "/_kosha/ui",
            get(|| async { Redirect::permanent("/_kosha/ui/") }),
        )
        .route("/_kosha/ui/", get(|| serve(String::new())))
        .route("/_kosha/ui/{*path}", get(|Path(p): Path<String>| serve(p)))
}

async fn serve(path: String) -> Response {
    let path = if path.is_empty() {
        "index.html".to_string()
    } else {
        path
    };
    // Unknown paths are client-side routes: serve the app shell.
    let (file, name) = match Assets::get(&path) {
        Some(f) => (f, path),
        None => match Assets::get("index.html") {
            Some(f) => (f, "index.html".to_string()),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    "The web console was not built into this binary. Run `npm ci && npm run build` in ui/ and rebuild.\n",
                )
                    .into_response();
            }
        },
    };
    let mime = mime_guess::from_path(&name).first_or_octet_stream();
    // Hashed asset names are immutable; the shell must always revalidate.
    let cache = if name.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    Response::builder()
        .header(header::CONTENT_TYPE, mime.as_ref())
        .header(header::CACHE_CONTROL, cache)
        .header("x-content-type-options", "nosniff")
        .body(Body::from(file.data.into_owned()))
        .unwrap()
}
