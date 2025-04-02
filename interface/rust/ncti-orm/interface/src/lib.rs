// use crate::executor::Executor;
#![allow(warnings, unused)]
#[allow(non_camel_case_types)]

extern crate libc;
use std::fmt::{Debug, Display};
use std::ffi::CString;
use std::ffi::CStr;
use libc::{c_char, c_int, c_uint};

use codegen;

pub mod data;
pub mod func;

// pub mod connection;

use crate::{data::exp_column_def_t, data::exp_index_def_t, data::GS_FALSE, data::GS_TRUE, data::dbtype_t, data::gs_type_t, data::assign_type_t, data::col_text_t, data::status_t, data::res_row_def_t};

#[link(name = "intarkdb")]

extern "C" {
    fn db_alloc(handle: *const u64) -> libc::c_int;

    fn intarkdb_shutdown_db();

    fn intarkdb_startup_db(dbtype: libc::c_int, data_path: *const c_char) -> status_t;
    
    fn db_open_table(handle: u64, table_name: *const c_char) -> libc::c_int;

    fn gstor_create_user_table(handle: u64, table_name: *const c_char, column_count: libc::c_int, column_list: *const exp_column_def_t, index_count: libc::c_int, index_list: *const exp_index_def_t) -> libc::c_int;
    fn gstor_create_index(handle: u64, table_name: *const c_char, index_def: *const exp_index_def_t) -> libc::c_int;

    fn gstor_insert_row(handle: u64, table_name: *const c_char, column_count: libc::c_int, column_list: *const exp_column_def_t) -> status_t;

    fn gstor_update_row(handle: u64, table_name: *const c_char, upd_column_count: libc::c_int, upd_column_list: *const exp_column_def_t, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t) -> libc::c_int;

    fn gstor_delete_row(handle: u64, table_name: *const c_char, del_count: *mut libc::c_int, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t) -> libc::c_int;

    fn db_open_cursor_ex(handle: u64, table_name: *const c_char, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t, eof: *mut libc::c_uint, idx_slot: libc::c_int) -> status_t;

    fn db_cursor_next(handle: u64, eof: *mut libc::c_uint) -> status_t;
 
    fn db_cursor_fetch(handle: u64, sel_column_count: libc::c_int, sel_column_list: *const exp_column_def_t, res_row_count: *mut libc::c_int, res_row_list: *mut res_row_def_t) -> status_t;

    // EXPORT_API status_t db_commit(void *handle);
    fn db_commit(handle: u64) -> status_t;

    // EXPORT_API status_t db_rollback(void *handle);
    fn db_rollback(handle: u64) -> status_t;

}

pub struct Interface {
    db_type: libc::c_int,
    path: String,
    // handle: Box::<u64>,
}

impl Interface {
    pub fn new(data_path: String, db_type_id: libc::c_int)  -> Self {
        return Self {
            db_type: db_type_id,
            path: data_path,
            // handle: Box::<u64>::new(0),
        }
    }

    pub fn open_db(&self) -> i32 {
        let ret = unsafe {
            intarkdb_startup_db(self.db_type as libc::c_int, CString::new(self.path.clone()).unwrap().into_raw())
        };
        if status_t::GS_SUCCESS == ret {
          //  println!("open db success! db path is {}",self.path);
            // self.init_handle();
        } else {
            println!("open db error! db path is {}",self.path);
        }
        ret as i32   
    }

    pub fn init_handle(&self, handle: *const u64) {
        let ret = unsafe {
            db_alloc(handle)
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
          //  println!("alloc db handle success!");
        } else {
            println!("alloc db handle fail!");
        }

    }

    fn open_table_with_name(&self, handle: u64, table_name: &String) -> i32 {
        let ret = unsafe {
            db_open_table(handle, CString::new(table_name.clone()).unwrap().into_raw())
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
          //  println!("db open table success! table name is {}", table_name);
        } else {
            println!("db open table fail! table name is {}", table_name);
        }
        ret
    }

    fn create_table(&self, handle: u64, table_name: &String, column_count: libc::c_int, column_list: *const exp_column_def_t, index_count: libc::c_int, index_list: *const exp_index_def_t) -> i32{
        let ret = unsafe {
            gstor_create_user_table(handle, CString::new(table_name.clone()).unwrap().into_raw(), column_count, column_list, index_count, index_list)
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
             println!("db create table success! table name is {}", table_name);
        } else {
            println!("db create table fail! table name is {}", table_name);
        }
        ret
    }
    fn create_index(&self, handle: u64, table_name: &String, index_def: *const exp_index_def_t) -> i32 {

        let ret = unsafe {
            gstor_create_index(handle, CString::new(table_name.clone()).unwrap().into_raw(), index_def)
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
            
            // println!("db create table index success! table name is {}, index is {:?}", table_name, index_def);
        } else {
            println!("db create table index fail! table name is {}, index is {:?} ", table_name, index_def);
        }
        ret
    }

    fn insert_row(&self, handle: u64, table_name: &String, column_count: libc::c_int, column_list: *const exp_column_def_t) -> i32 {
        let ret = unsafe {
            gstor_insert_row(handle, CString::new(table_name.clone()).unwrap().into_raw(), column_count, column_list)
        };
        if status_t::GS_SUCCESS as status_t == ret {
           // println!("db insert row success! table name is {}", table_name);
        } else {
            println!("db insert row fail! table name is {}", table_name);
        }
        ret as i32
    }

    fn update_row(&self, handle: u64, table_name: &String, upd_column_count: libc::c_int, upd_column_list: *const exp_column_def_t, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t) -> i32 {
        let ret = unsafe {
            gstor_update_row(handle, CString::new(table_name.clone()).unwrap().into_raw(), upd_column_count, upd_column_list, cond_column_count, cond_column_list)
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
           // println!("db update row success! table name is {}", table_name);
        } else {
            println!("db update row fail! table name is {}", table_name);
        }
        ret
    }
    fn delete_row(&self, handle: u64, table_name: &String, del_count: *mut libc::c_int, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t) -> i32 {
        let ret = unsafe {
            gstor_delete_row(handle, CString::new(table_name.clone()).unwrap().into_raw(),  del_count, cond_column_count, cond_column_list)
        };
        if status_t::GS_SUCCESS as libc::c_int == ret {
           // println!("db delete row success! table name is {}", table_name);
        } else {
            println!("db delete row fail! table name is {}", table_name);
        }
        ret
    }

    fn open_cursor(&self, handle: u64, table_name: &String, cond_column_count: libc::c_int, cond_column_list: *const exp_column_def_t, eof: *mut libc::c_uint, idx_slot: libc::c_int) -> i32 {
        let ret = unsafe {
            db_open_cursor_ex(handle, CString::new(table_name.clone()).unwrap().into_raw(), cond_column_count, cond_column_list, eof, idx_slot)
        };
        if status_t::GS_SUCCESS == ret {
          //  println!("db open cursor success! table name is {}", table_name);
        } else {
            println!("db open cursor fail! table name is {}", table_name);
        }
        ret as i32
    }

    fn cursor_next(&self, handle: u64, eof: *mut libc::c_uint) -> i32 {
        let ret = unsafe {
            db_cursor_next(handle, eof)
        };
        // if status_t::GS_SUCCESS == ret {
        //     println!("db cursor next success!");
        // } else {
        //     println!("db cursor next fail! ");
        // }
        ret as i32
    }

    fn cursor_fetch(&self, handle: u64, sel_column_count: libc::c_int, sel_column_list: *const exp_column_def_t, res_row_count: *mut libc::c_int, res_row_list: *mut res_row_def_t ) -> i32 {
        let ret = unsafe {
            db_cursor_fetch(handle, sel_column_count, sel_column_list, res_row_count, res_row_list)
        };
        // if status_t::GS_SUCCESS == ret {
        //     println!("db cursor fetch success!");
        // } else {
        //     println!("db cursor fetch fail!");
        // }
        ret as i32
    }

    pub fn db_commit(&self, handle: u64) {
        let ret = unsafe {
            db_commit(handle)
        };
        // if status_t::GS_SUCCESS == ret {
        //     println!("db commit success!");
        // } else {
        //     println!("db commit fail!");
        // }
    }

    pub fn db_rollback(&self, handle: u64) {
        let ret = unsafe {
            db_rollback(handle)
        };
        // if status_t::GS_SUCCESS == ret {
        //     println!("db rollback success!");
        // } else {
        //     println!("db rollback fail!");
        // }
    }

    pub fn db_shutdown(&self) {
        unsafe {
            intarkdb_shutdown_db()
        };
      //  println!("db shutdown success!");
    }


}