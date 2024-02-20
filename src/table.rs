use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlparser::ast::ColumnDef;

use crate::parser::expression::Literal;

pub type RowId = usize;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_map: BTreeMap<PKType, usize>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub header: ColumnHeader,
    pub data: ColumnData,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Int,
    Str,
    Float,
    Double,
    Bool,
    Invalid,
}

impl From<&ColumnData> for DataType {
    fn from(value: &ColumnData) -> Self {
        match value {
            ColumnData::Int(_) => Self::Int,
            ColumnData::Str(_) => Self::Str,
            ColumnData::Float(_) => Self::Float,
            ColumnData::Double(_) => Self::Double,
            ColumnData::Bool(_) => Self::Bool,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum PKType {
    Int(i32),
    Str(String),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnHeader {
    pub name: String,
    pub hidden: bool,
    pub datatype: DataType,
    pub nullable: bool,
    pub is_pk: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnData {
    Int(BTreeMap<RowId, i32>),
    Str(BTreeMap<RowId, String>),
    Float(BTreeMap<RowId, f32>),
    Double(BTreeMap<RowId, f64>),
    Bool(BTreeMap<RowId, bool>),
}

impl ColumnData {
    fn truncate(&mut self) {
        match self {
            ColumnData::Int(d) => d.clear(),
            ColumnData::Str(d) => d.clear(),
            ColumnData::Float(d) => d.clear(),
            ColumnData::Double(d) => d.clear(),
            ColumnData::Bool(d) => d.clear(),
        }
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<ColumnDef>) -> Self {
        let columns: Vec<Column> = columns
            .into_iter()
            .map(|c| {
                let data = match c.data_type {
                    sqlparser::ast::DataType::Varchar(_) => ColumnData::Str(Default::default()),
                    sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                        ColumnData::Int(Default::default())
                    }
                    sqlparser::ast::DataType::Float(_)
                    | sqlparser::ast::DataType::Float4
                    | sqlparser::ast::DataType::Real => ColumnData::Float(Default::default()),
                    sqlparser::ast::DataType::Float8
                    | sqlparser::ast::DataType::Float64
                    | sqlparser::ast::DataType::Double
                    | sqlparser::ast::DataType::DoublePrecision => {
                        ColumnData::Double(Default::default())
                    }
                    sqlparser::ast::DataType::Bool | sqlparser::ast::DataType::Boolean => {
                        ColumnData::Bool(Default::default())
                    }
                    _ => unimplemented!(),
                };

                let mut is_pk = false;
                let mut nullable = true;
                let mut unique = false;

                c.options.into_iter().for_each(|c| match c.option {
                    sqlparser::ast::ColumnOption::Null => {
                        nullable = true;
                    }
                    sqlparser::ast::ColumnOption::NotNull => {
                        nullable = false;
                    }
                    sqlparser::ast::ColumnOption::Unique { is_primary } => {
                        is_pk = is_primary;
                        unique = true;
                        nullable = false;
                    }
                    _ => unimplemented!(),
                });

                Column {
                    header: ColumnHeader {
                        name: c.name.to_string(),
                        nullable,
                        is_pk,
                        datatype: DataType::from(&data),
                        hidden: false,
                    },
                    data,
                }
            })
            .collect();

        log::debug!("creating table {name} with columns: {columns:?}");

        if !columns.iter().any(|c| c.header.is_pk) {
            log::error!("cannot create table with no primary key");
            panic!("cannot create table with no primary key");
        }

        Self {
            name,
            columns,
            pk_map: Default::default(),
        }
    }

    pub fn truncate(&mut self) {
        self.columns.iter_mut().for_each(|c| c.data.truncate());
    }

    pub fn col_from_name(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.header.name.to_lowercase() == name.to_lowercase())
    }
}
