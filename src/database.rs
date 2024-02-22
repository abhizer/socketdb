use crate::{
    evaluator::{Evaluator, OutColumn},
    metacommands::MetaCommand,
    parser::parser::{self, Query},
    table::Table,
    Error, Result,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    str::FromStr,
};

use flume::{Receiver, Sender};

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
    #[serde(skip)]
    receiver: Option<Receiver<(String, Sender<String>)>>,
    #[serde(skip)]
    ws_map: HashMap<String, Vec<Sender<String>>>,
}

#[derive(Debug, Default)]
pub struct View {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl View {
    pub fn new(cols: Vec<OutColumn>) -> Self {
        let columns = cols.iter().map(|c| c.name.clone()).collect();

        let max_rows = cols.iter().map(|c| c.data.len()).max().unwrap_or(0);

        let mut rows = Vec::new();
        for i in 0..=max_rows {
            let mut row = Vec::new();
            for col in &cols {
                row.push(col.data.get_as_string(i).unwrap_or_default());
            }

            // don't show empty rows
            if row.iter().all(|x| x.is_empty()) {
                continue;
            }

            rows.push(Row { items: row });
        }

        Self { columns, rows }
    }
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
            if !row.is_empty() {
                table.add_row(prettytable::Row::from_iter(&mut row.items.iter()));
            }
        }

        if val.rows.is_empty() {
            table.add_empty_row();
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

    pub fn recv_senders(&mut self) -> Result<()> {
        let Some(ref rx) = self.receiver else {
            return Ok(());
        };

        if let Ok((tbl_name, sender)) = rx.try_recv() {
            log::info!("subscribed to table: {tbl_name}");
            self.ws_map
                .entry(tbl_name)
                .and_modify(|v| v.push(sender.clone()))
                .or_insert(vec![sender]);
        }

        Ok(())
    }

    pub fn set_receiver(&mut self, receiver: Receiver<(String, Sender<String>)>) {
        self.receiver = Some(receiver);
    }

    pub fn execute(&mut self, query: Query) -> Result<Option<View>> {
        self.recv_senders()?;

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
                    Some(tbl) => {
                        tbl.truncate();
                        if let Some(txs) = self.ws_map.get(&tbl.name.to_lowercase()) {
                            for tx in txs {
                                _ = tx.send(format!("table: {tbl_name} truncated"));
                            }
                        }
                    }
                    None => Err(Error::TableNotFound(tbl_name))?,
                }
            }
            parser::Query::Select(select) => {
                let table = select.from.and_then(|name| {
                    self.tables
                        .iter()
                        .find(|t| name.to_lowercase() == t.name.to_lowercase())
                });

                // dear god this is dogshit
                // but I need to get this done by tomorrow

                let mut selected = Vec::new();
                let mut projected = Vec::new();

                for s in select.selection {
                    if matches!(s, crate::parser::expression::Expression::None) {
                        continue;
                    }

                    selected.extend(Evaluator::eval(table, s)?);
                }

                for p in select.projection {
                    projected.extend(Evaluator::eval(table, p)?);
                }

                log::debug!("selected: {selected:?}");
                log::debug!("projected: {projected:?}");

                let result = if selected.is_empty() {
                    // everything is selected
                    projected
                } else {
                    let mut res = Vec::new();
                    for p in projected {
                        for s in &selected {
                            let name = p.name.clone();
                            let keys: Vec<usize> = match &s.data {
                                crate::table::ColumnData::Bool(b) => {
                                    b.iter().filter(|(_, v)| **v).map(|(k, _)| *k).collect()
                                }
                                _ => panic!("not possible"),
                            };

                            log::debug!("selected keys: {keys:?}");

                            let mut data = p.data.clone();
                            data.retain_keys(&keys);

                            let col = OutColumn { name, data };

                            res.push(col);
                        }
                    }
                    res
                };

                log::debug!("result: {result:?}");

                return Ok(Some(View::new(result)));
            }
            Query::Insert {
                table,
                columns,
                sources,
            } => {
                match self
                    .tables
                    .iter_mut()
                    .find(|t| t.name.to_lowercase() == table.to_lowercase())
                {
                    Some(tbl) => {
                        tbl.insert(columns.clone(), sources.clone())?;

                        let outcols: Vec<OutColumn> =
                            tbl.columns.iter().map(OutColumn::from).collect();
                        let view = View::new(outcols);
                        if let Some(txs) = self.ws_map.get(&tbl.name.to_lowercase()) {
                            for tx in txs {
                                _ = tx.send(format!("table: {} updated\n {view}", tbl.name));
                                log::info!("sent insert updates");
                            }
                        }
                    }
                    None => Err(Error::TableNotFound(table))?,
                }
            }
            Query::Drop(table) => self
                .tables
                .retain(|t| t.name.to_lowercase() != table.to_lowercase()),
            Query::Update {
                table,
                assignments,
                selection,
            } => {
                let table = self
                    .tables
                    .iter_mut()
                    .find(|t| table.to_lowercase() == t.name.to_lowercase())
                    .ok_or(Error::TableNotFound(table))?;

                let selection = selection.ok_or(Error::Unsupported(
                    "update without selection (where)".to_string(),
                ))?;

                let selected = Evaluator::eval(Some(table), selection)?;
                if selected.len() != 1 {
                    return Err(Error::InvalidOperation(
                        "more than one column found in selection".to_owned(),
                    ));
                }
                let selected = selected[0].data.keys_where_true()?;

                table.update(assignments, selected)?;

                let outcols: Vec<OutColumn> = table.columns.iter().map(OutColumn::from).collect();
                let view = View::new(outcols);
                if let Some(txs) = self.ws_map.get(&table.name.to_lowercase()) {
                    for tx in txs {
                        _ = tx.send(format!("table: {} updated\n {view}", table.name));
                    }
                }
            }
            Query::Delete { table, selection } => {
                let table = self
                    .tables
                    .iter_mut()
                    .find(|t| table.to_lowercase() == t.name.to_lowercase())
                    .ok_or(Error::TableNotFound(table))?;

                if let Some(selection) = selection {
                    let selected = Evaluator::eval(Some(table), selection)?;
                    if selected.len() != 1 {
                        return Err(Error::InvalidOperation(
                            "more than one column found in selection".to_owned(),
                        ));
                    }
                    let selected = selected[0].data.keys_where_true()?;
                    table.delete(selected)?;

                    let outcols: Vec<OutColumn> =
                        table.columns.iter().map(OutColumn::from).collect();
                    let view = View::new(outcols);
                    if let Some(txs) = self.ws_map.get(&table.name.to_lowercase()) {
                        for tx in txs {
                            _ = tx
                                .send(format!("data deleted from table: {}\n {view}", table.name));
                        }
                    }
                } else {
                    table.truncate();
                }
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
            MetaCommand::Persist(path) => {
                let json = serde_json::to_string(&self).expect("failed to serialize the database");

                let file = File::create(path).expect("failed to create file");
                let buf = BufWriter::new(file);

                let mut encoder =
                    zstd::Encoder::new(buf, 3).expect("failed to create zstd compression ecoder");
                encoder
                    .write_all(json.as_bytes())
                    .expect("failed to write to file");
                encoder.finish().expect("failed to finish writing to file");
            }
            MetaCommand::Restore(path) => {
                let file = File::open(path).expect("failed to open file");
                let buf = BufReader::new(file);

                let decoded = zstd::decode_all(buf).expect("failed to decode db from disk");
                let db: Database =
                    serde_json::from_slice(&decoded).expect("db deserialization error");

                self.tables = db.tables;
            }
        }
    }
}
