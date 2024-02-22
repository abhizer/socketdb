use std::collections::HashMap;

use sqlparser::{
    ast::{ColumnDef, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};

use crate::{parser::expression::Expression, Error};

use super::{expression::Literal, select::Select};

#[derive(Debug)]
pub enum Query {
    Select(Select),
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        sources: Vec<Vec<Literal>>,
    },
    Update {
        table: String,
        assignments: HashMap<String, Literal>,
        selection: Option<Expression>,
    },
    Delete {
        table: String,
        selection: Option<Expression>,
    },
    Truncate(String),
    Drop(String),
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
        } => {
            if !into {
                return Err(Error::InvalidQuery(
                    "insert without into keyword".to_owned(),
                ));
            }

            let Some(source) = source else {
                return Err(Error::InvalidQuery(
                    "insert without data sources".to_owned(),
                ));
            };

            log::debug!("sources: {source:?}");

            let mut sources = Vec::new();
            match *source.body {
                sqlparser::ast::SetExpr::Values(v) => {
                    for outer in &v.rows {
                        let mut source_vec = Vec::new();
                        for e in outer {
                            match Expression::from_expr(e.clone())? {
                                Expression::Literal(l) => {
                                    source_vec.push(l);
                                }
                                _ => {
                                    return Err(Error::Unsupported(
                                        "values with non literals".to_owned(),
                                    ))
                                }
                            }
                        }
                        sources.push(source_vec);
                    }
                }
                _ => {
                    return Err(Error::Unsupported(
                        "insert without values not supported".to_owned(),
                    ))
                }
            };

            Ok(Query::Insert {
                table: table_name.to_string(),
                columns: columns.into_iter().map(|v| v.to_string()).collect(),
                sources,
            })
        }
        Statement::Update {
            table,
            assignments,
            from,
            selection,
            ..
        } => {
            if from.is_some() {
                return Err(Error::Unsupported("update with from table".to_owned()));
            }

            let tbl_name = match table.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                _ => {
                    return Err(Error::Unsupported(
                        "update with complex table relation".to_owned(),
                    ))
                }
            };

            log::info!("update: table name: {tbl_name}");

            let mut assign_map = HashMap::new();
            for assignment in assignments {
                if assignment.id.len() != 1 {
                    return Err(Error::Unsupported(
                        "update assignment with more than one id".to_owned(),
                    ));
                }
                let col_name = assignment.id[0].value.clone();
                let value = Expression::from_expr(assignment.value)?;

                if let Expression::Literal(l) = value {
                    assign_map.insert(col_name, l);
                } else {
                    return Err(Error::Unsupported(
                        "non literal in update query".to_string(),
                    ));
                }
            }

            let selection = if let Some(expr) = selection {
                Some(Expression::from_expr(expr)?)
            } else {
                None
            };

            Ok(Query::Update {
                table: tbl_name,
                assignments: assign_map,
                selection,
            })
        }
        Statement::Delete {
            from, selection, ..
        } => {
            log::info!("delete: from: {from:?}");
            log::info!("delete: selection: {selection:?}");

            if from.len() != 1 {
                return Err(Error::Unsupported(
                    "delete from more than one table".to_string(),
                ));
            }

            let tbl_name = match &from[0].relation {
                sqlparser::ast::TableFactor::Table { name, .. } => name.to_string(),
                _ => {
                    return Err(Error::Unsupported(
                        "delete with complex table relation".to_owned(),
                    ))
                }
            };

            let selection = if let Some(expr) = selection {
                Some(Expression::from_expr(expr)?)
            } else {
                None
            };

            Ok(Query::Delete {
                table: tbl_name,
                selection,
            })
        }
        Statement::Drop {
            object_type, names, ..
        } => match object_type {
            sqlparser::ast::ObjectType::Table => {
                if names.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "drop query must have one table name".to_owned(),
                    ));
                }

                let name = &names[0];

                Ok(Query::Drop(name.to_string()))
            }
            _ => Err(Error::InvalidOperation(
                "drop only allowed for tables".to_owned(),
            )),
        },
        _ => Err(Error::Unsupported(format!("unsupported statement: {stmt}"))),
    }
}
