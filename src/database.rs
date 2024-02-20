use crate::{
    evaluator::Evaluator,
    metacommands::MetaCommand,
    parser::parser::{self, Query},
    table::Table,
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    str::FromStr,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Row {
    items: Vec<String>,
}

impl Row {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Database {
    tables: Vec<Table>,
}

#[derive(Debug, Default)]
pub struct View {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl From<View> for prettytable::Table {
    fn from(val: View) -> Self {
        let mut table = prettytable::Table::new();

        table.add_row(prettytable::Row::from_iter(&mut val.columns.iter()));
        for row in val.rows {
            if row.is_empty() {
                table.add_empty_row();
            } else {
                table.add_row(prettytable::Row::from_iter(&mut row.items.iter()));
            }
        }

        table
    }
}

impl From<&View> for prettytable::Table {
    fn from(val: &View) -> Self {
        let mut table = prettytable::Table::new();

        table.add_row(prettytable::Row::from_iter(&mut val.columns.iter()));
        for row in val.rows.iter() {
            if row.is_empty() {
                table.add_empty_row();
            } else {
                table.add_row(prettytable::Row::from_iter(&mut row.items.iter()));
            }
        }

        table
    }
}

impl Display for View {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tbl = prettytable::Table::from(self);
        write!(f, "{}", tbl)
    }
}

impl Database {
    pub fn new() -> Self {
        log::debug!("creating a new database");
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        log::debug!("trying to open file: `{}`", &path.display());
        let file = File::open(path)?;

        let mut file = BufReader::new(file);

        let mut buf = Vec::new();

        file.read_to_end(&mut buf)?;

        log::debug!("deserializing from bincode");
        let db = bincode::deserialize(&buf)?;

        log::info!("opened database: `{}`", path.display());

        Ok(db)
    }

    pub fn execute(&mut self, query: Query) -> Result<Option<View>> {
        match query {
            parser::Query::CreateTable { name, columns } => {
                if self
                    .tables
                    .iter()
                    .any(|t| t.name.to_lowercase() == name.to_lowercase())
                {
                    log::error!("table {name} already exists");
                    return Err(Error::TableAlreadyExists(name));
                } else {
                    let table = Table::new(name.to_string().to_uppercase(), columns);
                    self.tables.push(table);
                    log::debug!("created table: {name}");
                }
            }
            parser::Query::Truncate(tbl_name) => {
                match self
                    .tables
                    .iter_mut()
                    .find(|t| t.name.to_lowercase() == tbl_name.to_lowercase())
                {
                    Some(tbl) => tbl.truncate(),
                    None => Err(Error::TableNotFound(tbl_name))?,
                }
            }
            parser::Query::Select(select) => {
                let table = select.from.and_then(|name| {
                    self.tables
                        .iter()
                        .find(|t| name.to_lowercase() == t.name.to_lowercase())
                });

                for p in select.projection {
                    let eval_res = Evaluator::eval(table, p)?;
                    log::debug!("{eval_res:?}");
                }

                // for s in select.selection {
                //     let eval_res = Evaluator::eval(table, s)?;
                //     log::debug!("{eval_res:?}");
                // }
            }
        }
        Ok(None)
    }

    pub fn execute_all(&mut self, query: &str) -> Result<()> {
        if let Ok(meta) = MetaCommand::from_str(query) {
            self.metacommand_handler(meta);
            return Ok(());
        }

        let queries = parser::parse_all(query)?;

        for query in queries {
            if let Some(view) = self.execute(query)? {
                println!("{view}");
            }
        }

        Ok(())
    }
}

// Meta Commands
impl Database {
    fn metacommand_handler(&mut self, cmd: MetaCommand) {
        match cmd {
            MetaCommand::ListTables => {
                let mut tbl = prettytable::Table::new();
                tbl.add_row(prettytable::row!["name", "columns"]);

                self.tables
                    .iter()
                    .map(|t| {
                        let col_names = t
                            .columns
                            .iter()
                            .map(|c| &c.header.name)
                            .fold("".to_string(), |acc, i| format!("{acc}{i}\n"));
                        prettytable::row![t.name, col_names]
                    })
                    .for_each(|r| {
                        tbl.add_row(r);
                    });

                println!("{tbl}");
            }

            MetaCommand::Exit => std::process::exit(0),
        }
    }
}
