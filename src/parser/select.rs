use crate::Error;

use super::expression::Expression;
use sqlparser::ast::Query;

pub struct Select {
    pub from: Option<String>,
    pub projection: Vec<Expression>,
    pub selection: Vec<Expression>,
}

impl Select {
    pub fn new(query: Query) -> Result<Self, Error> {
        let mut from = None;
        let mut projection = Vec::new();
        let mut selection = Vec::new();

        match *query.body {
            sqlparser::ast::SetExpr::Select(select) => {
                let select = *select;
                for p in select.projection {
                    match p {
                        sqlparser::ast::SelectItem::UnnamedExpr(exp) => {
                            let exp = Expression::from_expr(exp)?;
                            projection.push(exp);
                        }
                        sqlparser::ast::SelectItem::Wildcard(_) => {
                            projection.push(Expression::Ident(super::expression::Ident::Wildcard));
                        }
                        _ => Err(Error::Unsupported(format!("selection item: {p}")))?,
                    }
                }

                if let Some(f) = select.from.into_iter().next() {
                    match f.relation {
                        sqlparser::ast::TableFactor::Table { name, .. } => {
                            from = Some(name.to_string());
                        }
                        _ => Err(Error::Unsupported(format!("relation: {}", f.relation)))?,
                    }
                }

                let sel = match select.selection {
                    Some(exp) => Expression::from_expr(exp)?,
                    None => Expression::None,
                };

                selection.push(sel);
            }
            _ => Err(Error::Unsupported(format!("query body: {}", query.body)))?,
        }

        Ok(Self {
            from,
            projection,
            selection,
        })
    }
}
