#[macro_use]
extern crate serde;
extern crate core;

pub mod index;
#[allow(deprecated)]
pub mod value;

pub use crate::value::ext::Error;
pub use value::ext::{deserialize_from, from_value};
pub use value::ext::{to_value, to_value_def};
pub use value::Value;

impl Value {
    pub fn into_ext(self, name: &'static str) -> Self {
        match self {
            Value::Ext(_, _) => self,
            _ => Value::Ext(name, Box::new(self)),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Value::Null => true,
            Value::Bool(_) => false,
            Value::I32(_) => false,
            Value::I64(_) => false,
            Value::U32(_) => false,
            Value::U64(_) => false,
            Value::F32(_) => false,
            Value::F64(_) => false,
            Value::String(v) => v.is_empty(),
            Value::Binary(v) => v.is_empty(),
            Value::Array(v) => v.is_empty(),
            Value::Map(v) => v.is_empty(),
            Value::Ext(_, v) => v.is_empty(),
        }
    }
}
//注解说明宏应该是可用的。 如果没有该注解，这个宏不能被引入作用域。
#[macro_export]
//使用 macro_rules! 和宏名称开始宏定义，且所定义的宏并 不带 感叹号。名字后跟大括号表示宏定义体，在该例中宏名称是 to_value 。
macro_rules! to_value {
    ($arg:expr) => {
        $crate::to_value($arg).unwrap_or_default()
    };
}
