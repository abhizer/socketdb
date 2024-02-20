use sqlparser::{
    ast::{ColumnDef, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::Error;

use super::select::Select;

pub enum Query {
    Select(Select),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    Truncate(String),
}

pub fn parse_all(query: &str) -> Result<Vec<Query>, Error> {
    let mut res = Vec::new();

    let ast = Parser::parse_sql(&PostgreSqlDialect {}, query)?;

    for stmt in ast {
        let query = parse(stmt)?;
        res.push(query);
    }

    Ok(res)
}

pub fn parse(stmt: Statement) -> Result<Query, Error> {
    match stmt {
        Statement::CreateTable { name, columns, .. } => Ok(Query::CreateTable {
            name: name.to_string(),
            columns,
        }),
        Statement::Truncate { table_name, .. } => Ok(Query::Truncate(table_name.to_string())),
        Statement::Query(q) => Ok(Query::Select(Select::new(*q)?)),
        Statement::Insert {
            into,
            table_name,
            columns,
            source,
            ..
        } => todo!(),
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            ..
        } => todo!(),
        Statement::Delete {
            tables,
            from,
            using,
            selection,
            returning,
            order_by,
            limit,
        } => todo!(),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            restrict,
            purge,
            temporary,
        } => todo!(),
        _ => unimplemented!(),
    }
}
