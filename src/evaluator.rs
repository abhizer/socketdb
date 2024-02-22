use std::collections::BTreeMap;

use crate::parser::expression::{Expression, Literal};
use crate::table::{Column, ColumnData, Table};
use crate::{Error, Result};

pub struct Evaluator;

#[derive(Debug, Clone)]
pub struct OutColumn {
    pub name: String,
    pub data: ColumnData,
}

impl From<Literal> for OutColumn {
    fn from(value: Literal) -> Self {
        let data = match value {
            Literal::Int(l) => {
                let mut map = BTreeMap::default();
                map.insert(0, l);
                ColumnData::Int(map)
            }
            Literal::Str(s) => {
                let mut map = BTreeMap::default();
                map.insert(0, s);
                ColumnData::Str(map)
            }
            Literal::Bool(b) => {
                let mut map = BTreeMap::default();
                map.insert(0, b);
                ColumnData::Bool(map)
            }
            Literal::Float(f) => {
                let mut map = BTreeMap::default();
                map.insert(0, f);
                ColumnData::Float(map)
            }
            Literal::Double(d) => {
                let mut map = BTreeMap::default();
                map.insert(0, d);
                ColumnData::Double(map)
            }
            Literal::Null => unreachable!(),
        };

        data.into()
    }
}

impl From<&Column> for OutColumn {
    fn from(value: &Column) -> Self {
        Self {
            name: value.header.name.to_string(),
            data: value.data.to_owned(),
        }
    }
}

impl From<ColumnData> for OutColumn {
    fn from(value: ColumnData) -> Self {
        Self {
            name: "?column?".to_string(),
            data: value,
        }
    }
}

impl Evaluator {
    pub fn eval(table: Option<&Table>, expr: Expression) -> Result<Vec<OutColumn>> {
        match expr {
            Expression::Literal(l) => {
                let data = match l {
                    Literal::Null => vec![],
                    _ => vec![l.into()],
                };

                Ok(data)
            }
            Expression::Ident(id) => {
                let Some(table) = table else {
                    return Err(Error::EvaluationError(
                        "cannot evaluate identifier without table".to_owned(),
                    ));
                };

                match id {
                    crate::parser::expression::Ident::Wildcard => {
                        Ok(table.columns.iter().map(|c| c.into()).collect())
                    }
                    crate::parser::expression::Ident::Named(id) => Ok({
                        let col = table.col_from_name(&id).ok_or(Error::ColumnNotFound {
                            col: id,
                            table: table.name.clone(),
                        })?;
                        vec![col.into()]
                    }),
                }
            }
            Expression::IsFalse(expr) => {
                match *expr {
                    Expression::Literal(l) => match l {
                        Literal::Bool(b) => {
                            let out = if !b {
                                table
                                    .map(|t| {
                                        t.columns
                                            .iter()
                                            .map(|c| c.into())
                                            .collect::<Vec<OutColumn>>()
                                    })
                                    .unwrap_or_default()
                            } else {
                                Vec::default()
                            };
                            Ok(out)
                        }
                        _ => Err(Error::InvalidOperation(
                            "is false on non boolean literals".to_string(),
                        )),
                    },
                    Expression::Ident(id) => match id {
                        crate::parser::expression::Ident::Wildcard => Err(Error::InvalidOperation(
                            "is false with wildcard (*) operator".to_string(),
                        )),
                        crate::parser::expression::Ident::Named(id) => {
                            let Some(table) = table else {
                                return Err(Error::InvalidOperation(format!(
                                    "is false with identifier {id} with no table"
                                )));
                            };

                            let Some(col) = table.col_from_name(&id) else {
                                return Err(Error::ColumnNotFound {
                                    col: id.clone(),
                                    table: table.name.clone(),
                                });
                            };

                            let out: OutColumn = match col.data {
                                ColumnData::Bool(ref tree) => {
                                    let tree = tree.iter().filter(|(_, v)| !**v).map(|(k, v)| (*k, *v)).collect();
                                    ColumnData::Bool(tree).into()
                                },
                                _ => return Err(Error::InvalidOperation("cannot apply `is false` in column with datatype other than bool".to_string())),
                            };

                            Ok(vec![out])
                        }
                    },
                    _ => Err(Error::Unsupported(
                        "is false with other than literal or identifier".to_owned(),
                    )),
                }
            }
            Expression::IsTrue(expr) => {
                match *expr {
                    Expression::Literal(l) => match l {
                        Literal::Bool(b) => {
                            let out = if !b {
                                table
                                    .map(|t| {
                                        t.columns
                                            .iter()
                                            .map(|c| c.into())
                                            .collect::<Vec<OutColumn>>()
                                    })
                                    .unwrap_or_default()
                            } else {
                                Vec::default()
                            };
                            Ok(out)
                        }
                        _ => Err(Error::InvalidOperation(
                            "is true on non boolean literals".to_string(),
                        )),
                    },
                    Expression::Ident(id) => match id {
                        crate::parser::expression::Ident::Wildcard => Err(Error::InvalidOperation(
                            "is true with wildcard (*) operator".to_string(),
                        )),
                        crate::parser::expression::Ident::Named(id) => {
                            let Some(table) = table else {
                                return Err(Error::InvalidOperation(format!(
                                    "is true with identifier {id} with no table"
                                )));
                            };

                            let Some(col) = table.col_from_name(&id) else {
                                return Err(Error::ColumnNotFound {
                                    col: id.clone(),
                                    table: table.name.clone(),
                                });
                            };

                            let out: OutColumn = match col.data {
                                ColumnData::Bool(ref tree) => {
                                    let tree = tree.iter().filter(|(_, v)| **v).map(|(k, v)| (*k, *v)).collect();
                                    ColumnData::Bool(tree).into()
                                },
                                _ => return Err(Error::InvalidOperation("cannot apply `is true` in column with datatype other than bool".to_string())),
                            };

                            Ok(vec![out])
                        }
                    },
                    _ => Err(Error::Unsupported(
                        "is true with other than literal or identifier".to_owned(),
                    )),
                }
            }
            Expression::Unary {
                operator,
                expression,
            } => match operator {
                crate::parser::expression::Unary::Not => match *expression {
                    Expression::Literal(l) => match l {
                        Literal::Bool(b) => Ok(vec![Literal::Bool(!b).into()]),
                        _ => todo!(),
                    },
                    Expression::Ident(id) => match id {
                        crate::parser::expression::Ident::Named(id) => {
                            let Some(table) = table else {
                                return Err(Error::Unsupported(
                                    "identifier without column name to apply unary operator"
                                        .to_string(),
                                ));
                            };
                            let col = table.col_from_name(&id).unwrap();
                            if let ColumnData::Bool(tree) = &col.data {
                                let tree = tree.iter().map(|(k, v)| (*k, !*v)).collect();
                                Ok(vec![ColumnData::Bool(tree).into()])
                            } else {
                                Err(Error::Unsupported(
                                    "not operator on non boolean column".to_string(),
                                ))
                            }
                        }
                        crate::parser::expression::Ident::Wildcard => todo!(),
                    },
                    _ => todo!(),
                },
                crate::parser::expression::Unary::Plus => match *expression {
                    Expression::Literal(l) => Ok(vec![l.into()]),
                    Expression::Ident(ident) => Evaluator::eval(table, Expression::Ident(ident)),
                    _ => Err(Error::Unsupported(
                        "unary operator plus on non literal or non column".to_owned(),
                    )),
                },
                crate::parser::expression::Unary::Minus => match *expression {
                    Expression::Literal(l) => {
                        let l = match l {
                            Literal::Int(i) => Literal::Int(-i),
                            Literal::Float(f) => Literal::Float(-f),
                            Literal::Double(d) => Literal::Double(-d),
                            Literal::Null => Literal::Null,
                            Literal::Str(_) | Literal::Bool(_) => {
                                return Err(Error::Unsupported(
                                    "unary operator minus on non numeric type".to_owned(),
                                ))
                            }
                        };
                        Ok(vec![l.into()])
                    }
                    Expression::Ident(ident) => {
                        let out_col = Evaluator::eval(table, Expression::Ident(ident))?;
                        let mut out = vec![];

                        for mut c in out_col {
                            c.data = match c.data {
                                ColumnData::Int(i) => {
                                    ColumnData::Int(i.into_iter().map(|(k, v)| (k, -v)).collect())
                                }
                                ColumnData::Float(f) => {
                                    ColumnData::Float(f.into_iter().map(|(k, v)| (k, -v)).collect())
                                }
                                ColumnData::Double(d) => ColumnData::Double(
                                    d.into_iter().map(|(k, v)| (k, -v)).collect(),
                                ),
                                ColumnData::Bool(_) | ColumnData::Str(_) => {
                                    return Err(Error::Unsupported(
                                        "unary operator minus on non numeric type column"
                                            .to_owned(),
                                    ))
                                }
                            };
                            out.push(c);
                        }

                        Ok(out)
                    }
                    _ => Err(Error::Unsupported(
                        "unary operator on non literal or column".to_owned(),
                    )),
                },
            },
            Expression::Binary {
                operator,
                left: left_expr,
                right: right_expr,
            } => {
                // TODO: avoid infinite loop by checking the variant
                let left = Evaluator::eval(table, *left_expr)?;
                let right = Evaluator::eval(table, *right_expr.clone())?;

                if left.len() != 1 || right.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "binary operator with more than one column".to_owned(),
                    ));
                }
                let left = &left[0];
                let right = &right[0];

                let out = match operator {
                    crate::parser::expression::Binary::Plus => match (&left.data, &right.data) {
                        (ColumnData::Str(left), ColumnData::Str(right)) => ColumnData::Str(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, format!("{lv}{rv}")))
                                .collect(),
                        ),
                        (ColumnData::Int(left), ColumnData::Int(right)) => ColumnData::Int(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv + rv))
                                .collect(),
                        ),
                        (ColumnData::Float(left), ColumnData::Float(right)) => ColumnData::Float(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv + rv))
                                .collect(),
                        ),
                        (ColumnData::Double(left), ColumnData::Double(right)) => {
                            ColumnData::Double(
                                left.iter()
                                    .zip(right)
                                    .filter(|((lk, _), (rk, _))| lk == rk)
                                    .map(|((lk, lv), (_, rv))| (*lk, lv + rv))
                                    .collect(),
                            )
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "binary op add between two different types".to_owned(),
                            ))
                        }
                    },

                    crate::parser::expression::Binary::Minus => match (&left.data, &right.data) {
                        (ColumnData::Int(left), ColumnData::Int(right)) => ColumnData::Int(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv - rv))
                                .collect(),
                        ),
                        (ColumnData::Float(left), ColumnData::Float(right)) => ColumnData::Float(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv - rv))
                                .collect(),
                        ),
                        (ColumnData::Double(left), ColumnData::Double(right)) => {
                            ColumnData::Double(
                                left.iter()
                                    .zip(right)
                                    .filter(|((lk, _), (rk, _))| lk == rk)
                                    .map(|((lk, lv), (_, rv))| (*lk, lv - rv))
                                    .collect(),
                            )
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "binary op minus on invalid type".to_owned(),
                            ))
                        }
                    },

                    crate::parser::expression::Binary::Mul => match (&left.data, &right.data) {
                        (ColumnData::Int(left), ColumnData::Int(right)) => ColumnData::Int(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv * rv))
                                .collect(),
                        ),
                        (ColumnData::Float(left), ColumnData::Float(right)) => ColumnData::Float(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv * rv))
                                .collect(),
                        ),
                        (ColumnData::Double(left), ColumnData::Double(right)) => {
                            ColumnData::Double(
                                left.iter()
                                    .zip(right)
                                    .filter(|((lk, _), (rk, _))| lk == rk)
                                    .map(|((lk, lv), (_, rv))| (*lk, lv * rv))
                                    .collect(),
                            )
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "binary op mul on invalid type".to_owned(),
                            ))
                        }
                    },

                    crate::parser::expression::Binary::Div => match (&left.data, &right.data) {
                        (ColumnData::Int(left), ColumnData::Int(right)) => ColumnData::Int(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv / rv))
                                .collect(),
                        ),
                        (ColumnData::Float(left), ColumnData::Float(right)) => ColumnData::Float(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv / rv))
                                .collect(),
                        ),
                        (ColumnData::Double(left), ColumnData::Double(right)) => {
                            ColumnData::Double(
                                left.iter()
                                    .zip(right)
                                    .filter(|((lk, _), (rk, _))| lk == rk)
                                    .map(|((lk, lv), (_, rv))| (*lk, lv / rv))
                                    .collect(),
                            )
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "binary op div on invalid type".to_owned(),
                            ))
                        }
                    },

                    crate::parser::expression::Binary::Rem => match (&left.data, &right.data) {
                        (ColumnData::Int(left), ColumnData::Int(right)) => ColumnData::Int(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv % rv))
                                .collect(),
                        ),
                        (ColumnData::Float(left), ColumnData::Float(right)) => ColumnData::Float(
                            left.iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv % rv))
                                .collect(),
                        ),
                        (ColumnData::Double(left), ColumnData::Double(right)) => {
                            ColumnData::Double(
                                left.iter()
                                    .zip(right)
                                    .filter(|((lk, _), (rk, _))| lk == rk)
                                    .map(|((lk, lv), (_, rv))| (*lk, lv % rv))
                                    .collect(),
                            )
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "binary op modulo on invalid type".to_owned(),
                            ))
                        }
                    },

                    crate::parser::expression::Binary::Eq => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let eq = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv == rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv == rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv == rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv == rv))
                                .collect(),
                            (ColumnData::Str(left), ColumnData::Str(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv == rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op equals on invalid type".to_owned(),
                                ))
                            }
                        };
                        log::debug!("eq: {eq:?}");
                        ColumnData::Bool(eq)
                    }

                    crate::parser::expression::Binary::Lt => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let lt = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv < rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv < rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv < rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv < rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op less than on invalid type".to_owned(),
                                ))
                            }
                        };
                        ColumnData::Bool(lt)
                    }

                    crate::parser::expression::Binary::Gt => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let gt = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv > rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv > rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv > rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv > rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op greater than on invalid type".to_owned(),
                                ))
                            }
                        };
                        ColumnData::Bool(gt)
                    }

                    crate::parser::expression::Binary::LtEq => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let lteq = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op less than eq on invalid type".to_owned(),
                                ))
                            }
                        };
                        ColumnData::Bool(lteq)
                    }

                    crate::parser::expression::Binary::GtEq => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let gteq = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv <= rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op greater than eq on invalid type".to_owned(),
                                ))
                            }
                        };
                        ColumnData::Bool(gteq)
                    }

                    crate::parser::expression::Binary::NotEq => {
                        let right_data = if let Expression::Literal(right_lit) = *right_expr {
                            ColumnData::fill_with_literal(right_lit, left.data.len())?
                        } else {
                            right.data.clone()
                        };

                        let neq = match (&left.data, &right_data) {
                            (ColumnData::Int(left), ColumnData::Int(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv != rv))
                                .collect(),
                            (ColumnData::Float(left), ColumnData::Float(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv != rv))
                                .collect(),
                            (ColumnData::Double(left), ColumnData::Double(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv != rv))
                                .collect(),
                            (ColumnData::Bool(left), ColumnData::Bool(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv != rv))
                                .collect(),
                            (ColumnData::Str(left), ColumnData::Str(right)) => left
                                .iter()
                                .zip(right)
                                .filter(|((lk, _), (rk, _))| lk == rk)
                                .map(|((lk, lv), (_, rv))| (*lk, lv != rv))
                                .collect(),
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "binary op not equal on invalid type".to_owned(),
                                ))
                            }
                        };
                        ColumnData::Bool(neq)
                    }
                };

                Ok(vec![OutColumn {
                    name: left.name.clone(),
                    data: out,
                }])
            }
            Expression::None => Err(Error::InvalidOperation("none operation".to_owned())),
            _ => Err(Error::Unsupported("unsupported query".to_owned())),
        }
    }
}
