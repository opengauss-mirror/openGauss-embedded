extern crate libc;
use libc::{c_char, c_int, c_uint};

pub const GS_FALSE:u32 = 0;
pub const GS_TRUE:u32 = 1;

// #[default] GS_TYPE_VARCHAR;
#[derive(Debug, Clone, Copy)]
// #[derive(#[default] GS_TYPE_VARCHAR)]
#[repr(C)]
pub enum gs_type_t {
    GS_TYPE_UNKNOWN = -1,
    GS_TYPE_BASE = 20000,
    GS_TYPE_INTEGER = 20000 + 1,    /* native 32 bits integer */ // u8 u16 u32
    GS_TYPE_BIGINT = 20000 + 2,     /* native 64 bits integer */
    GS_TYPE_REAL = 20000 + 3,       /* 8-byte native double */
    GS_TYPE_NUMBER = 20000 + 4,     /* number */
    GS_TYPE_DECIMAL = 20000 + 5,    /* decimal, internal used */
    GS_TYPE_DATE = 20000 + 6,       /* datetime */
    GS_TYPE_TIMESTAMP = 20000 + 7,  /* timestamp */
    GS_TYPE_CHAR = 20000 + 8,       /* char(n) */
    GS_TYPE_VARCHAR = 20000 + 9,    /* varchar, varchar2 */
    GS_TYPE_STRING = 20000 + 10,    /* native char * */
    GS_TYPE_BINARY = 20000 + 11,    /* binary */
    GS_TYPE_VARBINARY = 20000 + 12, /* varbinary */
    GS_TYPE_CLOB = 20000 + 13,      /* clob */
    GS_TYPE_BLOB = 20000 + 14,      /* blob */
    GS_TYPE_CURSOR = 20000 + 15,    /* resultset, for stored procedure */
    GS_TYPE_COLUMN = 20000 + 16,    /* column type, internal used */
    GS_TYPE_BOOLEAN = 20000 + 17,

    /* timestamp with time zone ,this type is fake, it is abandoned now,
    * you can treat it as GS_TYPE_TIMESTAMP just for compatibility */
    GS_TYPE_TIMESTAMP_TZ_FAKE  = 20000 + 18,
    GS_TYPE_TIMESTAMP_LTZ = 20000 + 19, /* timestamp with local time zone */
    GS_TYPE_INTERVAL = 20000 + 20,      /* interval of Postgre style, no use */
    GS_TYPE_INTERVAL_YM = 20000 + 21,   /* interval YEAR TO MONTH */
    GS_TYPE_INTERVAL_DS = 20000 + 22,   /* interval DAY TO SECOND */
    GS_TYPE_RAW = 20000 + 23,           /* raw */
    GS_TYPE_IMAGE = 20000 + 24,         /* image, equals to longblob */
    GS_TYPE_UINT32 = 20000 + 25,        /* unsigned integer */
    GS_TYPE_UINT64 = 20000 + 26,        /* unsigned bigint */
    GS_TYPE_SMALLINT = 20000 + 27,      /* 16-bit integer */
    GS_TYPE_USMALLINT = 20000 + 28,     /* unsigned 16-bit integer */
    GS_TYPE_TINYINT = 20000 + 29,       /* 8-bit integer */
    GS_TYPE_UTINYINT = 20000 + 30,      /* unsigned 8-bit integer */
    GS_TYPE_FLOAT = 20000 + 31,         /* 4-byte float */
    // !!!add new member must ensure not exceed the limitation of g_type_maps in sql_oper_func.c
    /* the real tz type , GS_TYPE_TIMESTAMP_TZ_FAKE will be not used , it will be the same as GS_TYPE_TIMESTAMP */
    GS_TYPE_TIMESTAMP_TZ  = 20000 + 32, /* timestamp with time zone */
    GS_TYPE_ARRAY = 20000 + 33,         /* array */
    /* com */
    /* caution: SCALAR type must defined above */
    GS_TYPE_OPERAND_CEIL = 20000 + 40,   // ceil of operand type

    /* The datatype can't used in datatype caculation system. only used for
    * decl in/out param in pl/sql */
    GS_TYPE_RECORD       = 20000 + 41,
    GS_TYPE_COLLECTION = 20000 + 42,
    GS_TYPE_OBJECT = 20000 + 43,
    /* The datatype below the GS_TYPE__DO_NOT_USE can be used as database DATATYPE.
    * In some extend, GS_TYPE__DO_NOT_USE represents the maximal number
    * of DATATYPE that Zenith are supported. The newly adding datatype
    * must before GS_TYPE__DO_NOT_USE, and the type_id must be consecutive
    */
    GS_TYPE__DO_NOT_USE = 20000 + 44,

    /* The following datatypes are functional datatypes, which can help
    * to implement some features when needed. Note that they can not be
    * used as database DATATYPE */
    /* to present a datatype node, for example cast(para1, typenode),
    * the second argument is an expr_node storing the information of
    * a datatype, such as length, precision, scale, etc.. */
    GS_TYPE_FUNC_BASE  = 20000 + 200,
    GS_TYPE_TYPMODE = 20000 + 200  + 1,

    /* This datatype only be used in winsort aggr */
    GS_TYPE_VM_ROWID = 20000 + 200  + 2,
    GS_TYPE_ITVL_UNIT = 20000 + 200  + 3,
    GS_TYPE_UNINITIALIZED = 20000 + 200  + 4,

    /* The following datatypes be used for native date or timestamp type value to bind */
    GS_TYPE_NATIVE_DATE = 20000 + 200  + 5,      // native datetime, internal used
    GS_TYPE_NATIVE_TIMESTAMP = 20000 + 200  + 6, // native timestamp, internal used
    GS_TYPE_LOGIC_TRUE = 20000 + 200  + 7,      // native true, internal used

}

// #[allow(non_camel_case_types)]
#[repr(C)]
pub enum table_type_t {
    TABLE_TYPE_HEAP = 0,
    TABLE_TYPE_IOT = 1,
    TABLE_TYPE_TRANS_TEMP = 2,
    TABLE_TYPE_SESSION_TEMP = 3,
    TABLE_TYPE_NOLOGGING = 4,
    TABLE_TYPE_EXTERNAL = 5,
}

#[repr(C)]
pub enum dbtype_t {
    DB_TYPE_GSTOR = 0,
    DB_TYPE_CEIL  = 1,
}

#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
pub enum assign_type_t {
    #[default]
    ASSIGN_TYPE_EQUAL = 0,                          //等于
    ASSIGN_TYPE_LESS = 0 + 1,       //小于
    ASSIGN_TYPE_MORE = 0 + 2,       //大于
    ASSIGN_TYPE_LESS_EQUAL = 0 + 3, //小于等于
    ASSIGN_TYPE_MORE_EQUAL = 0 + 4, //大于等于
    ASSIGN_TYPE_UNEQUAL = 0 + 5,    //不等于
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct col_text_t {
    pub str: *mut c_char, 
    pub len: u32,
    pub assign: assign_type_t,
} 

#[derive(Clone, Debug, Copy)]
#[repr(C)]
pub struct exp_column_def_t {
    pub name: col_text_t,           //字段名
    pub col_type: gs_type_t,        //字段类型
    pub col_slot: u16,         //字段槽位
    pub size: u16,          //字段长度
    pub nullable: u32,      //是否可空
    pub is_primary: u32,    //是否主键字段
    pub is_default: u32,    //是否有默认值
    pub default_val: col_text_t,    //默认值
    pub crud_value: col_text_t,     //列值（查询、插入、修改、删除时用到）
    pub precision: u16,     //精度（即小数点后的位数）
    pub comment: col_text_t,        //备注
}

#[derive(PartialEq, Eq)]
#[repr(C)]
pub enum status_t {
    GS_ERROR = -1,
    GS_SUCCESS = 0,
    GS_TIMEDOUT = 1,
} 

#[repr(C)]
pub struct exp_index_def_t {
    pub name: col_text_t,	       //索引名
    pub cols: *const col_text_t,	       //索引列列表
    pub col_count: u32,	  //索引列列数
    pub is_unique: u32,	  //是否唯一索引
    pub is_primary: u32,	 //是否主键索引
}

#[repr(C)]
pub struct res_row_def_t
{
    pub column_count: i32,                   //行数
    pub row_column_list: *mut exp_column_def_t,  //行包含的列列表
}

impl assign_type_t{

    pub fn as_u8(&self) -> u8 {
        match *self {
            assign_type_t::ASSIGN_TYPE_EQUAL => {
                return 0;
            },
            assign_type_t::ASSIGN_TYPE_LESS => {
                return 1;
            },
            assign_type_t::ASSIGN_TYPE_MORE => {
                return 2;
            },
            assign_type_t::ASSIGN_TYPE_LESS_EQUAL => {
                return 3;
            },
            assign_type_t::ASSIGN_TYPE_MORE_EQUAL => {
                return 4;
            },
            assign_type_t::ASSIGN_TYPE_UNEQUAL => {
                return 5;
            },
            _ => {
                return 0;
            },
        }
    }
}

impl From<u8> for assign_type_t {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0 => {
                return assign_type_t::ASSIGN_TYPE_EQUAL;
            },
            1 => {
                return assign_type_t::ASSIGN_TYPE_LESS;
            },
            2 => {
                return assign_type_t::ASSIGN_TYPE_MORE;
            },
            3 => {
                return assign_type_t::ASSIGN_TYPE_LESS_EQUAL;
            },
            4 => {
                return assign_type_t::ASSIGN_TYPE_MORE_EQUAL;
            },
            5 => {
                return assign_type_t::ASSIGN_TYPE_UNEQUAL;
            },
            _ => {
                return assign_type_t::ASSIGN_TYPE_EQUAL;;
            },
        } 
    }
}

impl gs_type_t {
    pub fn as_string(&self) -> String {
        match *self {
            gs_type_t::GS_TYPE_UNKNOWN => {    return String::from("UNKNOWN");
            },
            gs_type_t::GS_TYPE_BASE => {
                return String::from("BASE");    
            },
            gs_type_t::GS_TYPE_INTEGER => {
                return String::from("INTEGER");    
            },
            gs_type_t::GS_TYPE_BIGINT => {
                return String::from("BIGINT");    
            },
            gs_type_t::GS_TYPE_REAL => {
                return String::from("REAL");    
            },
            gs_type_t::GS_TYPE_NUMBER => {
                return String::from("NUMBER");    
            },
            gs_type_t::GS_TYPE_DECIMAL => {
                return String::from("DECIMAL");    
            },
            gs_type_t::GS_TYPE_DATE => {
                return String::from("DATE");    
            },
            gs_type_t::GS_TYPE_TIMESTAMP => {
                return String::from("TIMESTAMP");    
            },
            gs_type_t::GS_TYPE_CHAR => {
                return String::from("CHAR");    
            },
            gs_type_t::GS_TYPE_VARCHAR => {
                return String::from("VARCHAR");    
            },
            gs_type_t::GS_TYPE_STRING => {
                return String::from("STRING");    
            },
            gs_type_t::GS_TYPE_BINARY => {
                return String::from("BINARY");    
            },
            gs_type_t::GS_TYPE_VARBINARY => {
                return String::from("VARBINARY");    
            },
            gs_type_t::GS_TYPE_CLOB => {
                return String::from("CLOB");    
            },
            gs_type_t::GS_TYPE_BLOB => {
                return String::from("BLOB");    
            },
            gs_type_t::GS_TYPE_CURSOR => {
                return String::from("CURSOR");    
            },
            gs_type_t::GS_TYPE_COLUMN => {
                return String::from("COLUMN");    
            },
            gs_type_t::GS_TYPE_BOOLEAN => {
                return String::from("BOOLEAN");    
            },
            gs_type_t::GS_TYPE_TIMESTAMP_TZ_FAKE => {
                return String::from("TIMESTAMP_TZ_FAKE");    
            },
            gs_type_t::GS_TYPE_TIMESTAMP_LTZ => {
                return String::from("TIMESTAMP_LTZ");    
            },
            gs_type_t::GS_TYPE_INTERVAL => {
                return String::from("INTERVAL");    
            },
            gs_type_t::GS_TYPE_INTERVAL_YM => {
                return String::from("INTERVAL_YM");    
            },
            gs_type_t::GS_TYPE_INTERVAL_DS => {
                return String::from("INTERVAL_DS");    
            },
            gs_type_t::GS_TYPE_RAW => {
                return String::from("RAW");    
            },
            gs_type_t::GS_TYPE_IMAGE => {
                return String::from("IMAGE");    
            },
            gs_type_t::GS_TYPE_UINT32 => {
                return String::from("UINT32");    
            },
            gs_type_t::GS_TYPE_UINT64 => {
                return String::from("UINT64");    
            },
            gs_type_t::GS_TYPE_SMALLINT => {
                return String::from("SMALLINT");    
            },
            gs_type_t::GS_TYPE_USMALLINT => {
                return String::from("USMALLINT");    
            },
            gs_type_t::GS_TYPE_TINYINT => {
                return String::from("TINYINT");    
            },
            gs_type_t::GS_TYPE_UTINYINT => {
                return String::from("UTINYINT");    
            },
            gs_type_t::GS_TYPE_FLOAT => {
                return String::from("FLOAT");    
            },
            gs_type_t::GS_TYPE_TIMESTAMP_TZ => {
                return String::from("TIMESTAMP_TZ");    
            },
            gs_type_t::GS_TYPE_ARRAY => {
                return String::from("ARRAY");    
            },
            gs_type_t::GS_TYPE_OPERAND_CEIL => {
                return String::from("OPERAND_CEIL");    
            },
            gs_type_t::GS_TYPE_RECORD => {
                return String::from("RECORD");    
            },
            gs_type_t::GS_TYPE_COLLECTION => {
                return String::from("COLLECTION");    
            },
            gs_type_t::GS_TYPE_OBJECT => {
                return String::from("OBJECT");    
            },
            gs_type_t::GS_TYPE__DO_NOT_USE => {
                return String::from("_DO_NOT_USE");    
            },
            gs_type_t::GS_TYPE_FUNC_BASE => {
                return String::from("FUNC_BASE");    
            },
            gs_type_t::GS_TYPE_TYPMODE => {
                return String::from("TYPMODE");    
            },
            gs_type_t::GS_TYPE_VM_ROWID => {
                return String::from("VM_ROWID");    
            },
            gs_type_t::GS_TYPE_ITVL_UNIT => {
                return String::from("ITVL_UNIT");    
            },
            gs_type_t::GS_TYPE_UNINITIALIZED => {
                return String::from("UNINITIALIZED");    
            },
            gs_type_t::GS_TYPE_NATIVE_DATE => {
                return String::from("NATIVE_DATE");   
            },
            gs_type_t::GS_TYPE_NATIVE_TIMESTAMP => {
                return String::from("NATIVE_TIMESTAMP");
            },
            gs_type_t::GS_TYPE_LOGIC_TRUE => {
                return String::from("LOGIC_TRUE"); 
            },
            gs_type_t::GS_TYPE_UNKNOWN => {
                return String::from("UNKNOWN");  
            },   
        }
    }
}

impl From<&String> for gs_type_t{
    #[inline]
    fn from(v: &String) -> Self {
        match &v as &str {
            "UNKNOWN" => {
                return gs_type_t::GS_TYPE_UNKNOWN;
            },
            "BASE" => {
                return gs_type_t::GS_TYPE_BASE;
            },
            "INTEGER" => {
                return gs_type_t::GS_TYPE_INTEGER;
            },
            "BIGINT" => {
                return gs_type_t::GS_TYPE_BIGINT;
            },
            "REAL" => {
                return gs_type_t::GS_TYPE_REAL;
            },
            "NUMBER" => {
                return gs_type_t::GS_TYPE_NUMBER;
            },
            "DECIMAL" => {
                return gs_type_t::GS_TYPE_DECIMAL;
            },
            "DATE" => {
                return gs_type_t::GS_TYPE_DATE;
            },
            "TIMESTAMP" => {
                return gs_type_t::GS_TYPE_TIMESTAMP;
            },
            "CHAR" => {
                return gs_type_t::GS_TYPE_CHAR;
            },
            "VARCHAR" => {
                return gs_type_t::GS_TYPE_VARCHAR;
            },
            "STRING" => {
                return gs_type_t::GS_TYPE_STRING;
            },
            "BINARY" => {
                return gs_type_t::GS_TYPE_BINARY;
            },
            "VARBINARY" => {
                return gs_type_t::GS_TYPE_VARBINARY;
            },
            "CLOB" => {
                return gs_type_t::GS_TYPE_CLOB;
            },
            "BLOB" => {
                return gs_type_t::GS_TYPE_BLOB;
            },
            "CURSOR" => {
                return gs_type_t::GS_TYPE_CURSOR;
            },
            "COLUMN" => {
                return gs_type_t::GS_TYPE_COLUMN;
            },
            "BOOLEAN" => {
                return gs_type_t::GS_TYPE_BOOLEAN;
            },
            "TIMESTAMP_TZ_FAKE" => {
                return gs_type_t::GS_TYPE_TIMESTAMP_TZ_FAKE;
            },
            "TIMESTAMP_LTZ" => {
                return gs_type_t::GS_TYPE_TIMESTAMP_LTZ;
            },
            "INTERVAL" => {
                return gs_type_t::GS_TYPE_INTERVAL;
            },
            "INTERVAL_YM" => {
                return gs_type_t::GS_TYPE_INTERVAL_YM;
            },
            "INTERVAL_DS" => {
                return gs_type_t::GS_TYPE_INTERVAL_DS;
            },
            "RAW" => {
                return gs_type_t::GS_TYPE_RAW;
            },
            "IMAGE" => {
                return gs_type_t::GS_TYPE_IMAGE;
            },
            "UINT32" => {
                return gs_type_t::GS_TYPE_UINT32;
            },
            "UINT64" => {
                return gs_type_t::GS_TYPE_UINT64;
            },
            "SMALLINT" => {
                return gs_type_t::GS_TYPE_SMALLINT;
            },
            "USMALLINT" => {
                return gs_type_t::GS_TYPE_USMALLINT;
            },
            "TINYINT" => {
                return gs_type_t::GS_TYPE_TINYINT;
            },
            "UTINYINT" => {
                return gs_type_t::GS_TYPE_UTINYINT;
            },
            "FLOAT" => {
                return gs_type_t::GS_TYPE_FLOAT;
            },
            "TIMESTAMP_TZ" => {
                return gs_type_t::GS_TYPE_TIMESTAMP_TZ;
            },
            "ARRAY" => {
                return gs_type_t::GS_TYPE_ARRAY;
            },
            "OPERAND_CEIL" => {
                return gs_type_t::GS_TYPE_OPERAND_CEIL;
            },
            "RECORD" => {
                return gs_type_t::GS_TYPE_RECORD;
            },
            "COLLECTION" => {
                return gs_type_t::GS_TYPE_COLLECTION;
            },
            "OBJECT" => {
                return gs_type_t::GS_TYPE_OBJECT;
            },
            "_DO_NOT_USE" => {
                return gs_type_t::GS_TYPE__DO_NOT_USE;
            },
            "FUNC_BASE" => {
                return gs_type_t::GS_TYPE_FUNC_BASE;
            },
            "TYPMODE" => {
                return gs_type_t::GS_TYPE_TYPMODE;
            },
            "VM_ROWID" => {
                return gs_type_t::GS_TYPE_VM_ROWID;
            },
            "ITVL_UNIT" => {
                return gs_type_t::GS_TYPE_ITVL_UNIT;
            },
            "UNINITIALIZED" => {
                return gs_type_t::GS_TYPE_UNINITIALIZED;
            },
            "NATIVE_DATE" => {
                return gs_type_t::GS_TYPE_NATIVE_DATE;
            },
            "NATIVE_TIMESTAMP" => {
                return gs_type_t::GS_TYPE_NATIVE_TIMESTAMP;
            },
            "LOGIC_TRUE" => {
                return gs_type_t::GS_TYPE_LOGIC_TRUE;
            },
            _ => {
                return gs_type_t::GS_TYPE_UNKNOWN;
            }
        }
    }
    
}