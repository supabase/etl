use serde::Serialize;
use serde::de::DeserializeOwned;

/// Market trait that has to be implemented by configs that can be stored in the database.
///
/// With this trait we can enforce at compile time which structs can actually be stored and avoid
/// storing the wrong struct.
pub trait Store: Serialize + DeserializeOwned {}
