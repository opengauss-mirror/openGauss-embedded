use crate::utils::error::Error;
use rbs::Value;
use std::fmt::{Debug, Display, Formatter};



#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ExecResult {
    pub results: String,
    pub code: i32
}

impl Display for ExecResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .key(&"results")
            .value(&self.results)
            .key(&"code")
            .value(&self.code)
            .finish()
    }
}


/// Result set from executing a query against a statement
pub trait Row: 'static + Send + Debug {
    /// get meta data about this result set
    fn meta_data(&self) -> Box<dyn MetaData>;

    /// get Value from index
    fn get(&mut self, i: usize) -> Result<Value, Error>;
}

/// Meta data for result set
pub trait MetaData: Debug {
    fn column_len(&self) -> usize;
    fn column_name(&self, i: usize) -> String;
    fn column_type(&self, i: usize) -> String;
}
