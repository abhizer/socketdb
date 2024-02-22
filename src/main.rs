use std::time::{Duration, Instant};

use flume::{Receiver, Sender};

use actix::{Actor, ActorContext, AsyncContext, StreamHandler};
use actix_web::body::MessageBody;
use actix_web::http::StatusCode;
use actix_web::{get, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use actix_web_actors::ws;
use anyhow::Result;
use serde::Deserialize;
use socketdb::database::Database;

#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::from("SOCKET_DB_LOG_LEVEL")
            .default_filter_or("debug,rustyline=error,sqlparser=error"),
    )
    .init();

    log::info!("logger initialized");
    let (tx, rx) = flume::bounded(2);

    std::thread::spawn(move || {
        let mut rl = rustyline::DefaultEditor::new()?;

        let mut db = Database::new();
        db.set_receiver(rx);

        loop {
            match rl.readline(">> ") {
                Ok(line) => {
                    db.execute_all(line.trim())?;
                }
                Err(
                    rustyline::error::ReadlineError::Eof
                    | rustyline::error::ReadlineError::Interrupted,
                ) => {
                    break;
                }
                Err(err) => {
                    log::error!("error: {err}");
                    std::process::exit(1);
                }
            }
        }

        anyhow::Ok(())
    });

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState { sender: tx.clone() }))
            .service(index)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
    .map_err(|e| anyhow::anyhow!(e))
}

#[derive(Debug, Clone)]
struct AppState {
    sender: Sender<(String, Sender<String>)>, // table name, and the sender
}

struct Ws {
    receiver: Receiver<String>,
    start: Instant,
}

impl Actor for Ws {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |act, ctx| {
            if act.start.elapsed() > Duration::from_secs(60 * 60) {
                ctx.stop();
                return;
            }
            ctx.ping(b"");

            if let Ok(r) = act.receiver.try_recv() {
                ctx.text(r);
            }
        });
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for Ws {
    fn handle(&mut self, item: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        if let Ok(ws::Message::Ping(msg)) = item {
            ctx.pong(&msg)
        }
    }
}

#[derive(Deserialize)]
struct TableName {
    table: String,
}

#[get("/ws")]
async fn index(
    req: HttpRequest,
    query: web::Query<TableName>,
    state: web::Data<AppState>,
    stream: web::Payload,
) -> Result<HttpResponse, Error> {
    let username = req
        .headers()
        .get("ws-username")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let password = req
        .headers()
        .get("ws-password")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if username != "abhizer" && password != "passwd" {
        let resp = HttpResponse::new(StatusCode::UNAUTHORIZED);
        let resp = resp.set_body("invalid username or password".boxed());
        return Ok(resp);
    }

    let (tx, rx) = flume::bounded(2);

    state.sender.send((query.table.clone(), tx)).unwrap();

    ws::start(
        Ws {
            receiver: rx,
            start: Instant::now(),
        },
        &req,
        stream,
    )
}
