// use crate::executor::Executor;
#![allow(warnings, unused)]
#[allow(non_camel_case_types)]

extern crate libc;

use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::ffi::CString;
use std::ffi::CStr;
use std::process::id;
use std::str::FromStr;
use libc::{c_char, c_int, c_uint};
use std::slice;
use serde_json::{Value, json};
use codegen::utils::model::{StColumnDef4C, StIndexDef, Restriction};

use crate::{Interface, data::exp_column_def_t, data::exp_index_def_t, data::GS_FALSE, data::GS_TRUE, data::dbtype_t, data::gs_type_t, data::assign_type_t, data::col_text_t, data::status_t, data::res_row_def_t};

impl Interface {
    // pub fn open_db(&self) -> i32 {
    //     self.open_db_and_init()
    // }

    pub fn create_table_with_index(&self, handle: u64, table_name: &String, columndefs: & Vec<StColumnDef4C>, indexdefs: & Vec<StIndexDef>) -> i32 {

        let mut column_list: Vec<exp_column_def_t> = Vec::new();
        Interface::col_data_convert(&columndefs, &mut column_list);

        let mut index_list: Vec<exp_index_def_t> = Vec::new();
        Interface::index_data_convert(&indexdefs, &mut index_list);

        self.create_table(handle, table_name, column_list.len() as libc::c_int, column_list.as_ptr(), index_list.len() as libc::c_int,  index_list.as_ptr())
    }

    pub fn create_table_without_index(&self, handle: u64, table_name: &String, columndefs: & Vec<StColumnDef4C>) -> i32 {

        let mut column_list: Vec<exp_column_def_t> = Vec::new();

        Interface::col_data_convert(&columndefs, &mut column_list);
        
        self.create_table(handle, table_name, column_list.len() as libc::c_int, column_list.as_ptr(), 0,  std::ptr::null())

    }

    pub fn open_table(&self, handle: u64, table_name: &String) -> i32 {
        self.open_table_with_name(handle, table_name)
    }

    pub fn create_table_index(&self, handle: u64, table_name: &String, indexdef: & StIndexDef) -> i32 {
        let ret = self.open_table(handle, table_name);
        if status_t::GS_SUCCESS as libc::c_int != ret {
             println!("db open table fail! table name is {}", table_name);
            return ret;
        }

        let mut index_cols: Vec<col_text_t> = Vec::new();

        for cols_string in indexdef.cols.iter() {
            index_cols.push(
                col_text_t{
                    str:  CString::new(cols_string.clone()).unwrap().into_raw(),
                    len: cols_string.len() as u32,
                    assign: assign_type_t::ASSIGN_TYPE_EQUAL,
                }
            )
        }
        let mut index_def = exp_index_def_t{
            name: col_text_t{
                str:  CString::new(indexdef.name.clone()).unwrap().into_raw(),
                len: indexdef.name.len() as u32,
                assign: assign_type_t::ASSIGN_TYPE_EQUAL,
            }, 
            cols: index_cols.as_ptr(), 
            col_count:index_cols.len() as libc::c_uint, 
            is_unique: indexdef.is_unique as u32, 
            is_primary: indexdef.is_primary as u32,
        };
       let ret=  self.create_index(handle, table_name, &index_def);
    //    println!("create_index ret=== {:?}", ret);

       ret
       
    }

    pub fn intert_table_rows(&self, handle: u64, table_name: &String, columndefs: & Vec<StColumnDef4C>) -> i32 {

        let ret = self.open_table(handle, table_name);
        if status_t::GS_SUCCESS as libc::c_int != ret {
            // println!("db open table fail! table name is {}", table_name);
            return ret;
        }

        let mut column_list: Vec<exp_column_def_t> = Vec::new();

        Interface::col_data_convert(&columndefs, &mut column_list);

        self.insert_row(handle, table_name, column_list.len() as libc::c_int, column_list.as_ptr())

    }

    pub fn update_table_rows(&self, handle: u64, table_name: &String, upd_column_defs: & Vec<StColumnDef4C>, cond_column_defs: & Vec<StColumnDef4C>) -> i32 {

        let ret = self.open_table(handle, table_name);
        if status_t::GS_SUCCESS as libc::c_int != ret {
            // println!("db open table fail! table name is {}", table_name);
            return ret;
        }
        let mut upd_column_list: Vec<exp_column_def_t> = Vec::new();
        Interface::col_data_convert(&upd_column_defs, &mut upd_column_list);

        let mut cond_column_list: Vec<exp_column_def_t> = Vec::new();
        Interface::col_data_convert(&cond_column_defs, &mut cond_column_list);

         
        self.update_row(handle, table_name, upd_column_list.len() as libc::c_int, upd_column_list.as_ptr(), cond_column_list.len() as libc::c_int, cond_column_list.as_ptr())

    }

    pub fn table_delete_row(&self, handle: u64, table_name: &String, del_count: *mut libc::c_int, cond_column_defs: &Vec<StColumnDef4C>) -> i32 {
        let ret = self.open_table(handle, table_name);
        if status_t::GS_SUCCESS as libc::c_int != ret {
            // println!("db open table fail! table name is {}", table_name);
            return ret;
        }
        let mut cond_column_list: Vec<exp_column_def_t> = Vec::new();

        Interface::col_data_convert(&cond_column_defs, &mut cond_column_list);
        
        self.delete_row(handle, table_name, del_count, cond_column_list.len() as libc::c_int, cond_column_list.as_ptr())
    }



    pub fn table_fetch_data(&self, handle: u64, table_name: &String, cond_column_defs: & Vec<exp_column_def_t>, sel_column_defs: Vec<exp_column_def_t>,indexs_vec: &Vec<codegen::utils::model::StIndexDef>, res_row_defs: &mut Vec<serde_json::value::Value>) -> i32 {

        let ret = self.open_table(handle, table_name);
        if status_t::GS_SUCCESS as libc::c_int != ret {
            // println!("db open table fail! table name is {}", table_name);
            return ret;
        }
        let mut cond_column_defs_ref=cond_column_defs;
        let indexs_vec_ref=indexs_vec;

       // let mut cond_column_list: Vec<exp_column_def_t> = cond_column_defs.clone();
        let mut eof = GS_TRUE;
        //let cond_column_vec:Vec<exp_column_def_t>=cond_column_defs.clone();
        let vec_len=cond_column_defs_ref.len() as u32 ;
        let mut idx_slot:i32=-1;
        let mut idx_b:bool=false;//idx_b=false没有找到卡槽位置，全表搜索
        //println!("indexs_vec {:?}", indexs_vec);
        //找卡槽，组合索引->单索引->重新组合条件（去除找到卡的槽位条件）->筛选查询条件得到查询结果
        for index in indexs_vec_ref  {
            if vec_len==*&index.col_count{     //判读索引组合是否有多条件搜索条件一样的，如果一样走组合索引  搜索条件id=1，name=n  ，索引组合id，name
                let col_vec:&[String]=&*index.cols;
                let mut b:bool=true;
                for x in 0..cond_column_defs_ref.len() {
                    let name=unsafe {String::from_utf8_lossy(CStr::from_ptr(&*cond_column_defs_ref[x].name.str).to_bytes()).to_string()};
                    if  name !=col_vec[x] {
                        b=false;
                    }
                }
                if b {
                    idx_slot=index.idx_slot as i32;
                    idx_b=true;
                    break;
                }
            }
        }
        //搜索条件大于1，没有找到卡槽位
        let mut col_idx="".to_string();
        let mut cond_new:Vec<exp_column_def_t>=Vec::new();
        let mut idx_b2:bool=false;////没有找到卡槽位置，全表搜索
        if vec_len>1 && !idx_b {
            //多条件顺序从0开始寻找是否存在索引，根据这个索引条件去数据库查询数据，其他字段根据条件筛选查询结果
            for index in indexs_vec.clone() {
                if !idx_b2 {
                    for x in 0..cond_column_defs_ref.len() {
                        let index_vec = index.clone().cols;
                        let name=unsafe {String::from_utf8_lossy(CStr::from_ptr(&*cond_column_defs_ref[x].name.str).to_bytes()).to_string()};
                        if   name == index_vec[0] {
                            idx_slot = index.idx_slot as i32;
                            idx_b2 = true;
                            col_idx = index.name;
                            cond_new.push(cond_column_defs_ref[x]);
                            break;
                        }
                    }
                }
            }
        }

        if  idx_b2 {//重新组装索引查询条件
            cond_column_defs_ref=&cond_new;
        }
        // println!("idx_slot= {},idx_b={},idx_b2={},col_idx={}", idx_slot, idx_b,idx_b2,col_idx);
        let mut sel_column_list: Vec<exp_column_def_t> = sel_column_defs;
        let mut row_count = 0;
        let mut r_map:HashMap<String,String>=HashMap::new();
        //没有索引，idx_slot,搜索结果要根据查询条件做一次筛选，因为idx_slot==-1会全部返回表数据
        if idx_slot==-1{
            for cond in cond_column_defs {//r_map做查询结果的筛选条件
                let name=unsafe {String::from_utf8_lossy(CStr::from_ptr(cond.name.str).to_bytes()).to_string()};
                let crud_value=unsafe {String::from_utf8_lossy(CStr::from_ptr(cond.crud_value.str).to_bytes()).to_string()};
                r_map.insert(name,crud_value);
            }
        }else if idx_b2 { //多条件选择了一个条件作为索引搜索数据，r_map 除了索引条件，其他做查询结果的筛选条件
            for cond in cond_column_defs {
                let name=unsafe {String::from_utf8_lossy(CStr::from_ptr(cond.name.str).to_bytes()).to_string()};
                let crud_value=unsafe {String::from_utf8_lossy(CStr::from_ptr(cond.crud_value.str).to_bytes()).to_string()};
                if name!=col_idx{
                    r_map.insert(name,crud_value);
                }
            }
        }

            let mut ret = self.open_cursor(handle, table_name, cond_column_defs_ref.len() as libc::c_int, cond_column_defs_ref.as_ptr(), &mut eof, idx_slot);
            if status_t::GS_SUCCESS as libc::c_int != ret {
                println!("db open cursor fail! table name is {}", table_name);
                return ret
            }
      
            while (self.cursor_next(handle, &mut eof) ==  status_t::GS_SUCCESS as i32) {
               
             
                if(eof == GS_TRUE) {
                    //  println!("--db_cursor_next eof!!");
                    break;
                }
                let mut res_column_list = Vec::<exp_column_def_t>::with_capacity(sel_column_list.len());
                let mut res_row_list = res_row_def_t {
                    column_count: sel_column_list.len() as i32,
                    row_column_list: res_column_list.as_mut_ptr(),
                };

                let ret = self.cursor_fetch(handle, sel_column_list.len() as libc::c_int, sel_column_list.as_ptr(), &mut row_count, &mut res_row_list);

                if status_t::GS_SUCCESS as libc::c_int != ret {
                    println!("db cursor fetch fail!");
                    return ret
                }

                let mut vec = unsafe {
                    std::slice::from_raw_parts((res_row_list.row_column_list) as *const exp_column_def_t, res_row_list.column_count as usize).to_vec()
                };
                let mut i = 0;
                let mut jsonValue:serde_json::value::Value = json!({});
                let mut b=true;
                for row_info in &vec {

                    let  name=unsafe {String::from_utf8_lossy(CStr::from_ptr(row_info.name.str).to_bytes()).to_string()};
                    let val=  unsafe {String::from_utf8_lossy(CStr::from_ptr(row_info.crud_value.str).to_bytes()).to_string()};

                    //检查是否全表搜索或者多条件查询选择了一个索引
                    if idx_slot==-1 || idx_b2 {
                        //筛选符合查询条件的数据
                        if r_map.contains_key(&name) && r_map.get(&name).expect("REASON").to_string() !=val {
                            b=false;
                            break;
                        }
                    }
                    match row_info.col_type.as_string().as_str(){
                        "VARCHAR" => {
                            jsonValue.as_object_mut().unwrap().insert(name.to_string(), serde_json::value::Value::String(val));
                        },
                        "BIGINT" => {
                            let n=val.parse::<i32>().unwrap();
                            jsonValue.as_object_mut().unwrap().insert(name.to_string(), serde_json::value::Value::Number(n.into()));
                        },
                        _ => {
                            jsonValue.as_object_mut().unwrap().insert(name.to_string(), serde_json::value::Value::String(val));
                        },
                    }
                    i = i + 1;
                }
                if b{  res_row_defs.push(jsonValue);}
            }
        ret
    }

    fn col_data_convert(st_column_list: &Vec<StColumnDef4C>, exp_column_list: &mut Vec<exp_column_def_t>) {

        for column in st_column_list {
            exp_column_list.push(
                exp_column_def_t{
                    name: col_text_t {
                        str:  CString::new(column.name.clone()).unwrap().into_raw(),
                        len: column.name.len() as u32,
                        assign: assign_type_t::from(column.assign_type_t), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    }, 
                    col_type: gs_type_t::from(&column.types), //get_column_type(&column.types), //gs_type_t::GS_TYPE_VARCHAR, 
                    col_slot: column.col_slot,
                    size: column.size, 
                    nullable: column.nullable as u32, 
                    is_primary: column.is_primary as u32, 
                    is_default: column.is_default as u32, 
                    default_val: col_text_t {
                        str:  CString::new(column.default_val.clone()).unwrap().into_raw(),
                        len: column.default_val.len() as u32,
                        assign: assign_type_t::from(column.assign_type_t), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    }, 
                    crud_value: col_text_t {
                        str:  CString::new(column.crud_value.clone()).unwrap().into_raw(),
                        len: column.crud_value.len() as u32,
                        assign: assign_type_t::from(column.assign_type_t), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    }, 
                    precision: column.precision , 
                    comment: col_text_t {
                        str:  CString::new(column.comment.clone()).unwrap().into_raw(),
                        len: column.comment.len() as u32,
                        assign: assign_type_t::from(column.assign_type_t), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    },
                }
            );
        }
    }

    fn index_data_convert(st_index_list: &Vec<StIndexDef>, exp_index_list: &mut Vec<exp_index_def_t>) {

        for index in st_index_list {
            let mut index_cols: Vec<col_text_t> = Vec::new();
            for cols_string in index.cols.iter() {
                index_cols.push(
                    col_text_t{
                        str:  CString::new(cols_string.clone()).unwrap().into_raw(),
                        len: cols_string.len() as u32,
                        assign: assign_type_t::ASSIGN_TYPE_EQUAL,
                    }
                )
            }
            exp_index_list.push( 
                exp_index_def_t {
                    name: col_text_t{
                        str:  CString::new(index.name.clone()).unwrap().into_raw(),
                        len: index.name.len() as u32,
                        assign: assign_type_t::ASSIGN_TYPE_EQUAL,
                    }, 
                    cols: index_cols.as_ptr(), 
                    col_count:index_cols.len() as libc::c_uint, 
                    is_unique: index.is_unique as u32, 
                    is_primary: index.is_primary as u32,
                }
                
            )
        }
    }

}