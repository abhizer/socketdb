use std::collections::{BTreeMap, HashMap};

use bimap::BiBTreeMap;
use serde::{Deserialize, Serialize};
use sqlparser::ast::ColumnDef;

use crate::{parser::expression::Literal, Error};

pub type RowId = usize;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_map: BiBTreeMap<PKType, RowId>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub header: ColumnHeader,
    pub data: ColumnData,
}

impl Column {
    pub fn insert(&mut self, row_id: RowId, data: Literal) -> Result<(), Error> {
        self.header.last_row_id = Some(row_id);
        match (&mut self.data, data) {
            (ColumnData::Int(map), Literal::Int(d)) => {
                map.insert(row_id, d);
            }
            (ColumnData::Str(map), Literal::Str(d)) => {
                map.insert(row_id, d);
            }
            (ColumnData::Float(map), Literal::Float(d)) => {
                map.insert(row_id, d);
            }
            (ColumnData::Double(map), Literal::Double(d)) => {
                map.insert(row_id, d);
            }
            (ColumnData::Bool(map), Literal::Bool(d)) => {
                map.insert(row_id, d);
            }
            _ => return Err(Error::InvalidQuery("invalid data type".to_owned())),
        }

        Ok(())
    }
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
    pub last_row_id: Option<RowId>,
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
    pub fn update(&mut self, row_id: RowId, lit: Literal) -> Result<(), Error> {
        match (self, lit) {
            (ColumnData::Int(x), Literal::Int(value)) => {
                x.insert(row_id, value);
            }
            (ColumnData::Str(x), Literal::Str(value)) => {
                x.insert(row_id, value);
            }
            (ColumnData::Float(x), Literal::Float(value)) => {
                x.insert(row_id, value);
            }
            (ColumnData::Double(x), Literal::Double(value)) => {
                x.insert(row_id, value);
            }
            (ColumnData::Bool(x), Literal::Bool(value)) => {
                x.insert(row_id, value);
            }
            _ => {
                return Err(Error::InvalidOperation(
                    "invalid data type on update".to_owned(),
                ))
            }
        };

        Ok(())
    }

    pub fn delete(&mut self, row_id: RowId) {
        match self {
            ColumnData::Int(i) => {
                i.remove(&row_id);
            }
            ColumnData::Str(i) => {
                i.remove(&row_id);
            }
            ColumnData::Float(i) => {
                i.remove(&row_id);
            }
            ColumnData::Double(i) => {
                i.remove(&row_id);
            }
            ColumnData::Bool(i) => {
                i.remove(&row_id);
            }
        };
    }

    fn truncate(&mut self) {
        match self {
            ColumnData::Int(d) => d.clear(),
            ColumnData::Str(d) => d.clear(),
            ColumnData::Float(d) => d.clear(),
            ColumnData::Double(d) => d.clear(),
            ColumnData::Bool(d) => d.clear(),
        }
    }

    pub fn keys(&self) -> Vec<RowId> {
        match self {
            ColumnData::Int(x) => x.keys().cloned().collect(),
            ColumnData::Str(x) => x.keys().cloned().collect(),
            ColumnData::Float(x) => x.keys().cloned().collect(),
            ColumnData::Double(x) => x.keys().cloned().collect(),
            ColumnData::Bool(x) => x.keys().cloned().collect(),
        }
    }

    pub fn keys_where_true(&self) -> Result<Vec<RowId>, Error> {
        match self {
            ColumnData::Bool(map) => Ok(map.iter().filter(|(_, v)| **v).map(|(k, _)| *k).collect()),
            _ => Err(Error::InvalidOperation(
                "cannot select true only keys for non boolean".to_string(),
            )),
        }
    }

    pub fn retain_keys(&mut self, keys: &[RowId]) {
        match self {
            ColumnData::Int(d) => d.retain(|k, _| keys.contains(k)),
            ColumnData::Str(d) => d.retain(|k, _| keys.contains(k)),
            ColumnData::Float(d) => d.retain(|k, _| keys.contains(k)),
            ColumnData::Double(d) => d.retain(|k, _| keys.contains(k)),
            ColumnData::Bool(d) => d.retain(|k, _| keys.contains(k)),
        }
    }

    pub fn len(&self) -> RowId {
        match self {
            ColumnData::Int(d) => d.keys().max().copied().unwrap_or(0),
            ColumnData::Str(d) => d.keys().max().copied().unwrap_or(0),
            ColumnData::Float(d) => d.keys().max().copied().unwrap_or(0),
            ColumnData::Double(d) => d.keys().max().copied().unwrap_or(0),
            ColumnData::Bool(d) => d.keys().max().copied().unwrap_or(0),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            ColumnData::Int(d) => d.is_empty(),
            ColumnData::Str(d) => d.is_empty(),
            ColumnData::Float(d) => d.is_empty(),
            ColumnData::Double(d) => d.is_empty(),
            ColumnData::Bool(d) => d.is_empty(),
        }
    }

    pub fn get_as_string(&self, id: RowId) -> Option<String> {
        match self {
            ColumnData::Int(d) => d.get(&id).map(|v| v.to_string()),
            ColumnData::Str(d) => d.get(&id).map(|v| v.to_string()),
            ColumnData::Float(d) => d.get(&id).map(|v| v.to_string()),
            ColumnData::Double(d) => d.get(&id).map(|v| v.to_string()),
            ColumnData::Bool(d) => d.get(&id).map(|v| v.to_string()),
        }
    }

    pub fn fill_with_literal(lit: Literal, till: RowId) -> Result<Self, Error> {
        match lit {
            Literal::Int(x) => {
                let mut map = BTreeMap::default();
                for i in 0..=till {
                    map.insert(i, x);
                }
                Ok(Self::Int(map))
            }
            Literal::Str(x) => {
                let mut map = BTreeMap::default();
                for i in 0..=till {
                    map.insert(i, x.clone());
                }
                Ok(Self::Str(map))
            }
            Literal::Bool(x) => {
                let mut map = BTreeMap::default();
                for i in 0..=till {
                    map.insert(i, x);
                }
                Ok(Self::Bool(map))
            }
            Literal::Float(x) => {
                let mut map = BTreeMap::default();
                for i in 0..=till {
                    map.insert(i, x);
                }
                Ok(Self::Float(map))
            }
            Literal::Double(x) => {
                let mut map = BTreeMap::default();
                for i in 0..=till {
                    map.insert(i, x);
                }
                Ok(Self::Double(map))
            }
            Literal::Null => Err(Error::InvalidOperation(
                "cannot create a column data from null literal".to_owned(),
            )),
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
                        last_row_id: None,
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

    pub fn last_row_id(&self) -> Option<RowId> {
        self.columns
            .iter()
            .map(|c| c.header.last_row_id)
            .max()
            .flatten()
    }

    pub fn next_row_id(&self) -> RowId {
        self.last_row_id().map(|v| v + 1).unwrap_or(0)
    }

    pub fn truncate(&mut self) {
        self.columns.iter_mut().for_each(|c| c.data.truncate());
    }

    pub fn col_from_name(&self, name: &str) -> Option<&Column> {
        self.columns
            .iter()
            .find(|c| c.header.name.to_lowercase() == name.to_lowercase())
    }

    pub fn insert(
        &mut self,
        mut columns: Vec<String>,
        data: Vec<Vec<Literal>>,
    ) -> Result<(), Error> {
        if columns.is_empty() {
            columns = self.columns.iter().map(|c| c.header.name.clone()).collect();
        }

        let mut next_row_id = self.next_row_id();
        let mut cols: Vec<&mut Column> = self
            .columns
            .iter_mut()
            .filter(|c| columns.contains(&c.header.name))
            .collect();

        log::debug!("insert data: {data:?}");

        for datum in data {
            log::debug!("insert datum: {datum:?}");
            for (col, col_data) in cols.iter_mut().zip(datum) {
                log::debug!("insert col: {col:?}");
                log::debug!("insert col_data: {col_data:?}");
                col.insert(next_row_id, col_data)?;
            }
            next_row_id += 1;
        }

        log::debug!("column after inserting: {self:?}");

        Ok(())
    }

    pub fn update(
        &mut self,
        assignments: HashMap<String, Literal>,
        selected: Vec<RowId>,
    ) -> Result<(), Error> {
        for col in self.columns.iter_mut() {
            let Some(value) = assignments.get(&col.header.name.to_lowercase()) else {
                continue;
            };

            if col.header.is_pk {
                return Err(Error::Unsupported(
                    "updating the primary is not allowed".to_owned(),
                ));
            }

            for row_id in &selected {
                col.data.update(*row_id, value.clone())?;
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, selected: Vec<RowId>) -> Result<(), Error> {
        for row_id in selected {
            for col in self.columns.iter_mut() {
                col.data.delete(row_id);
            }

            self.pk_map.remove_by_right(&row_id);
        }

        Ok(())
    }
}
