#![allow(warnings, unused)]
extern crate libc;

use interface::data::res_row_def_t;
use libc::c_char;
use std::ffi::CStr;
use std::ffi::CString;
use std::vec;

use crate::interface::Interface;
use crate::interface::{data::dbtype_t, data::status_t, data::GS_FALSE, data::GS_TRUE};
use codegen::utils::model::{Restriction, StColumnDef, StColumnDef4C, StIndexDef};

fn test_open_db(conn: &mut Interface, table_name: &String) {
    // open db
    conn.open_db();
}


fn test_init(conn: &mut Interface, handle: *const u64) {
    // open db
    conn.init_handle(handle);
}

fn test_create_table(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut columndefs: Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    columndefs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("1"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 1,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("CHGAMT"),
        types: String::from("VARCHAR"),
        col_slot: 2,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("100"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });

    // conn.create_table_without_index(&table_name.clone(), &columndefs);

    let mut indexdef = codegen::utils::model::StIndexDef {
        name: String::from("IDX_GSTOR_LOG_TEST1_LOGID"),
        cols: vec![String::from("LOGID")],
        idx_slot: 0,
        col_count: 1,
        is_primary: GS_TRUE as u8,
        is_unique: GS_TRUE as u8,
    };

    let mut index_list = Vec::<StIndexDef>::new();
    index_list.push(indexdef);
    conn.create_table_with_index(handle, &table_name.clone(), &columndefs, &index_list);
}

fn test_create_index(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut indexdef = codegen::utils::model::StIndexDef {
        name: String::from("IDX_GSTOR_LOG_TEST1_ACCTID"),
        cols: vec![String::from("ACCTID")],
        idx_slot: 0,
        col_count: 1,
        is_primary: GS_FALSE as u8,
        is_unique: GS_FALSE as u8,
    };

    conn.create_table_index(handle, &table_name.clone(), &indexdef);
}

fn test_insert_row(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut columndefs: Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    columndefs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("1"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 1,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("CHGAMT"),
        types: String::from("VARCHAR"),
        col_slot: 2,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("100"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    if status_t::GS_SUCCESS as libc::c_int != conn.intert_table_rows(handle, &table_name.clone(), &columndefs){
        conn.db_rollback(handle);
    } else {
        conn.db_commit(handle);
    }
    
    for i in 1..10 {
        columndefs[0].crud_value = (1 + i).to_string();
        columndefs[1].crud_value = 10.to_string();
        columndefs[2].crud_value = (100 + i).to_string();

        if status_t::GS_SUCCESS as libc::c_int != conn.intert_table_rows(handle, &table_name.clone(), &columndefs) {
            conn.db_rollback(handle);
        } else {
            conn.db_commit(handle);
        }
    }

    

}

fn test_delete_row(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut cond_column_defs: Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    cond_column_defs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    println!("delete begin!");
    let mut del_count = 0;
    cond_column_defs[0].crud_value = String::from("10");
    let mut ret = conn.table_delete_row(handle, &table_name.clone(), &mut del_count, &cond_column_defs);
    println!("delete result = {}!", ret);
}

fn test_update_row(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut columndefs: Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    columndefs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("1"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 1,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 2,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("100"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });

    let mut cond_column_defs: Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    cond_column_defs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });

    columndefs[0].crud_value = String::from("101");
    columndefs[1].crud_value = String::from("102");
    columndefs[2].crud_value = String::from("1002");

    conn.update_table_rows(handle, &table_name.clone(), &columndefs, &cond_column_defs);
}

fn test_fetch_row(conn: &mut Interface, handle: u64, table_name: &String) {
    let mut columndefs: Vec<StColumnDef4C> = Vec::new();
    columndefs.push(StColumnDef4C {
        name: String::from("LOGID"),
        types: String::from("VARCHAR"),
        col_slot: 0,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_TRUE as u8,
        is_default: GS_TRUE as u8,
        crud_value: String::from("1"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 1,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });
    columndefs.push(StColumnDef4C {
        name: String::from("CHGAMT"),
        types: String::from("VARCHAR"),
        col_slot: 2,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("100"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });

    let mut cond_column_defs: Vec<StColumnDef4C> = Vec::new();
    cond_column_defs.push(StColumnDef4C {
        name: String::from("ACCTID"),
        types: String::from("VARCHAR"),
        col_slot: 1,
        size: 14,
        nullable: GS_FALSE as u8,
        is_primary: GS_FALSE as u8,
        is_default: GS_FALSE as u8,
        crud_value: String::from("10"),
        default_val: String::from(""),
        precision: 0,
        comment: String::from(""),
        assign_type_t: 0,
    });

    let mut res_row_defs: Vec<serde_json::value::Value> = Vec::new();

    // let mut ret = conn.table_fetch_data(handle, &table_name, &cond_column_defs, &columndefs, &mut res_row_defs);
    // println!("fetch table data result is {}, data len is {}", ret, res_row_defs.len());
    
    // for column in &res_row_defs {
    //     println!("select ROW: name: {:?}",column);
    // }

}
use std::io::stdin;
use std::io::stdout;
use std::io::Read;
use std::io::Write;

pub fn test_argv_db() {
    let mut buf = String::new();
    let std_in = std::io::stdin();
    print!("Please input the cmd:");
    std::io::stdout().flush();
    let data_path = String::from("./output/data");
    let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
    let mut table_name = String::from("GSTOR_LOG_TEST1");
    let mut handle = Box::<u64>::new(0);
    while std_in.read_line(&mut buf).is_ok() {
        // println!("cmd is: {}", buf);
        buf = buf.replace("\n", "");
        match &buf as &str {
            "exit" => break,
            "open" => {
                test_open_db(&mut conn, &table_name);
            }
            "init" => {
                test_init(&mut conn, handle.as_ref());
            }
            "create table" => {
                test_create_table(&mut conn, *handle, &table_name);
            }
            "create index" => {
                test_create_index(&mut conn, *handle, &table_name);
            }
            "insert" => {
                test_insert_row(&mut conn, *handle, &table_name);
            }
            "update" => {
                test_update_row(&mut conn, *handle, &table_name);
            }
            "delete" => {
                test_delete_row(&mut conn, *handle, &table_name);
            }
            "select" => {
                test_fetch_row(&mut conn, *handle, &table_name);
            }
            "select2" => {
                test_open_db(&mut conn, &table_name);
                test_create_table(&mut conn, *handle, &table_name);
                test_create_index(&mut conn, *handle, &table_name);
                test_insert_row(&mut conn, *handle, &table_name);
                test_fetch_row(&mut conn, *handle, &table_name);
            }
            _ => {}
        }
        buf.clear();
        print!("Please input the cmd:");
        std::io::stdout().flush();
    }
    conn.db_shutdown();
}
