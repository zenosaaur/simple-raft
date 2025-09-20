use std::collections::HashMap;
use std::str::FromStr;

#[derive(PartialEq, Eq, Hash, Debug, Clone, Copy)]
pub enum Table {
    User,
}

impl Table {
    pub fn primary_key(&self) -> Column {
        match self {
            Table::User => Column::Username,
        }
    }
}

impl FromStr for Table {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "user" => Ok(Table::User),
            _ => Err(()),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Column {
    Username,
    Password,
}

impl FromStr for Column {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "username" | "name" => Ok(Column::Username),
            "password" | "pass" => Ok(Column::Password),
            _ => Err(()),
        }
    }
}

type Row = HashMap<Column, String>;
type PrimaryKey = String;
type TableData = HashMap<PrimaryKey, Row>;

pub struct Db {
    tables: HashMap<Table, TableData>,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    pub fn new() -> Self {
        Db {
            tables: HashMap::new(),
        }
    }

    pub fn parse_command(&mut self, command: String) -> Result<String, String> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Command cannot be empty.".to_string());
        }

        let cmd = parts[0].to_uppercase();
        let args = &parts[1..];

        match cmd.as_str() {
            "SET" => self.set(args),
            "GET" => self.get(args),
            _ => Err(format!("Unknown command: {}", cmd)),
        }
    }

    pub fn set(&mut self, args: &[&str]) -> Result<String, String> {
        if args.len() != 4 {
            return Err("Format error. Expected: SET <table> <pk_value> <column> <value>".to_string());
        }

        let table = Table::from_str(args[0]).map_err(|_| "Invalid table name".to_string())?;
        let pk_value = args[1].to_string();
        let column = Column::from_str(args[2]).map_err(|_| "Invalid column name".to_string())?;
        let value = args[3].to_string();

        let pk_column = table.primary_key();

        let table_data = self.tables.entry(table).or_insert_with(HashMap::new);

        let row = table_data.entry(pk_value.clone()).or_insert_with(HashMap::new);

        if column == pk_column {
            row.insert(pk_column, pk_value.clone());
        }

        // Insert/update the column's value
        row.insert(column.clone(), value.clone());

        Ok(format!("OK. Set {:?} to '{}' for row '{}'", column, value, pk_value))
    }

    pub fn get(&self, args: &[&str]) -> Result<String, String> {
        if args.len() != 3 {
            return Err("Format error. Expected: GET <table> <pk_value> <column>".to_string());
        }

        let table = Table::from_str(args[0]).map_err(|_| "Invalid table name".to_string())?;
        let pk_value = args[1];
        let column_to_get = Column::from_str(args[2]).map_err(|_| "Invalid column name".to_string())?;

        self.tables
            .get(&table)
            .and_then(|table_data| table_data.get(pk_value))
            .and_then(|row| row.get(&column_to_get))
            .map(|value| value.clone())
            .ok_or_else(|| format!("No value found for key '{}' in table '{:?}'", pk_value, table))
    }
}