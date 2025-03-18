use serde::Serialize;
use serde::Deserialize;

#[derive(Debug, Default,Serialize, Deserialize)]
pub struct StColumnDef {
    pub name: String,
    pub types: String, //字符串VARCHAR,int BIGINT
    pub col_slot : u16,       //字段槽位------加一个字段槽位字段，也就是表字段的序号，从0开始
    pub size: u16,
    pub nullable: u8,
    pub is_primary: u8,
    pub is_default: u8,
    pub default_val: String,
    pub crud_value: String,
    pub precision: u16,
    pub comment: String
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StColumnDef4C {
    pub name: String,
    pub types: String, //字符串VARCHAR,int BIGINT
    pub col_slot : u16,       //字段槽位------加一个字段槽位字段，也就是表字段的序号，从0开始
    pub size: u16,
    pub nullable: u8,
    pub is_primary: u8,
    pub is_default: u8,
    pub default_val: String,
    pub crud_value: String,
    pub precision: u16,
    pub comment: String,
    pub assign_type_t: u8 //条件类型
}


#[derive(Debug, Default,Clone, Serialize, Deserialize)]
pub struct StIndexDef {
    pub name: String,
    pub cols: Vec<String>,
    pub idx_slot : u16,       //索引槽位------加一个索引槽位字段，也就是表索引的序号，从0开始
    pub col_count: u32,
    pub is_unique: u8,
    pub is_primary: u8
}


#[derive(Debug,Clone)]
pub struct Restriction{
    pub name: String,
    pub value: String,
    pub assign: u8
}
/**
    ASSIGN_TYPE_EQUAL = 0,                          //等于
    ASSIGN_TYPE_LESS = ASSIGN_TYPE_EQUAL + 1,       //小于
    ASSIGN_TYPE_MORE = ASSIGN_TYPE_EQUAL + 2,       //大于
    ASSIGN_TYPE_LESS_EQUAL = ASSIGN_TYPE_EQUAL + 3, //小于等于
    ASSIGN_TYPE_MORE_EQUAL = ASSIGN_TYPE_EQUAL + 4, //大于等于
eq	NOT EQUAL	等于
NE	NOT EQUAL	不等于
GT	GREATER THAN	大于
LT	LESS THAN	小于
GE	GREATER THAN OR EQUAL	大于等于
LE	LESS THAN OR EQUAL	小于等于
**/
impl Restriction {
    //构建结构体
    // pub fn new(name: String, value: String,assign : u8) -> Self {
    //     return Self { name, value,assign};
    // }
    //等于
    pub fn equ(name: String, value: String) -> Self {
        return Self { name, value, assign: 0 };
    }
    // //小于
    // pub fn lt(name: String, value: String) -> Self {
    //     return Self { name, value, assign: 1 };
    // }
    // //大于
    // pub fn gt(name: String, value: String) -> Self {
    //     return Self { name, value, assign: 2 };
    // }
    //
    // //小于等于
    // pub fn le(name: String, value: String) -> Self {
    //     return Self { name, value, assign: 3 };
    // }
    //
    // //大于等于
    // pub fn ge(name: String, value: String) -> Self {
    //     return Self { name, value, assign: 4 };
    // }
    //
    // //不等于
    // pub fn ne(name: String, value: String) ->  Self {
    //     return Self { name, value, assign: 5 };
    // }
}