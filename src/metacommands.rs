use std::str::FromStr;

use crate::Error;

pub enum MetaCommand {
    ListTables,
    Exit,
}

impl FromStr for MetaCommand {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ".exit" => Ok(MetaCommand::Exit),
            ".tables" => Ok(MetaCommand::ListTables),
            _ => Err(Error::InvalidMetaCommand(s.to_owned())),
        }
    }
}
