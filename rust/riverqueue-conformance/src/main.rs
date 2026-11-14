//! River Rust's conformance adapter: the contract defined by the Go package
//! `conformance/protocol`, which the harness in `conformance/harness` runs
//! against River Go's adapter. One server, generic over the backend, serves
//! both Postgres and SQLite through River's `Client`.

mod backend;
mod protocol;
mod server;
mod worker;

use std::{env, process::ExitCode, time::Duration};

use riverqueue::BoxError;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::{
    backend::{Backend, Postgres, Sqlite},
    protocol::{Request, Response, RpcError},
    server::Server,
};

/// How long stopping the client and rolling back transactions may take once
/// stdin closes.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("River Rust conformance adapter: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), BoxError> {
    let url = env::var("RIVER_CONFORMANCE_DATABASE_URL")
        .map_err(|_| "RIVER_CONFORMANCE_DATABASE_URL is required")?;
    let application_name = env::var("RIVER_CONFORMANCE_APPLICATION_NAME").unwrap_or_default();
    match env::var("RIVER_CONFORMANCE_DRIVER")
        .unwrap_or_default()
        .as_str()
    {
        "postgres" => serve(Postgres::connect(&url, &application_name)?).await,
        "sqlite" => serve(Sqlite::connect(&url, &application_name)?).await,
        driver => Err(format!("unsupported RIVER_CONFORMANCE_DRIVER {driver:?}").into()),
    }
}

/// Answers requests from stdin, one per line, until it closes.
async fn serve<B: Backend>(backend: B) -> Result<(), BoxError> {
    let mut server = Server::new(backend);
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    let outcome = async {
        while let Some(line) = lines.next_line().await? {
            let mut response = serde_json::to_vec(&respond(&mut server, &line).await)?;
            response.push(b'\n');
            stdout.write_all(&response).await?;
            stdout.flush().await?;
        }
        Ok::<_, BoxError>(())
    }
    .await;
    let _ = tokio::time::timeout(SHUTDOWN_TIMEOUT, server.shutdown()).await;
    outcome
}

async fn respond<B: Backend>(server: &mut Server<B>, line: &str) -> Response {
    let mut response = Response {
        error: None,
        id: 0,
        jsonrpc: "2.0",
        result: None,
    };
    let request: Request = match serde_json::from_str(line) {
        Ok(request) => request,
        Err(error) => {
            response.error = Some(RpcError::new(RpcError::PARSE_ERROR, error));
            return response;
        }
    };
    response.id = request.id;
    if request.jsonrpc != "2.0" {
        response.error = Some(RpcError::new(
            RpcError::INVALID_REQUEST,
            "jsonrpc must be 2.0",
        ));
        return response;
    }
    match server
        .handle(&request.method, request.params.as_deref())
        .await
    {
        Ok(result) => response.result = Some(result),
        Err(error) => response.error = Some(error),
    }
    response
}
