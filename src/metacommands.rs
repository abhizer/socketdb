use std::{path::PathBuf, str::FromStr};

use crate::Error;

pub enum MetaCommand {
    ListTables,
    Persist(PathBuf),
    Restore(PathBuf),
    Exit,
}

impl FromStr for MetaCommand {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let splitted: Vec<&str> = s.split(' ').collect();
        let first = splitted.first().unwrap_or(&".exit");

        match *first {
            ".exit" => Ok(MetaCommand::Exit),
            ".tables" => Ok(MetaCommand::ListTables),
            ".persist" => {
                let path = splitted.get(1).ok_or(Error::InvalidMetaCommand(
                    "persist is expected to be followed by a path".to_owned(),
                ))?;
                let path = PathBuf::from_str(path).unwrap();

                Ok(MetaCommand::Persist(path))
            }
            ".restore" => {
                let path = splitted.get(1).ok_or(Error::InvalidMetaCommand(
                    "restore is expected to be followed by a path".to_owned(),
                ))?;
                let path = PathBuf::from_str(path).unwrap();

                Ok(MetaCommand::Restore(path))
            }
            _ => Err(Error::InvalidMetaCommand(s.to_owned())),
        }
    }
}
