use anyhow::Result;
use socketdb::database::Database;

fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::from("SOCKET_DB_LOG_LEVEL")
            .default_filter_or("debug,rustyline=error,sqlparser=error"),
    )
    .init();

    log::info!("logger initialized");

    let mut rl = rustyline::DefaultEditor::new()?;

    let mut db = Database::new();

    loop {
        match rl.readline(">> ") {
            Ok(line) => {
                db.execute_all(line.trim())?;
            }
            Err(
                rustyline::error::ReadlineError::Eof | rustyline::error::ReadlineError::Interrupted,
            ) => {
                break;
            }
            Err(err) => {
                log::error!("error: {err}");
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
