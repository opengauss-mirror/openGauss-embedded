extern crate proc_macro;
extern crate regex;

use crate::proc_macro::TokenStream;
use quote::quote;
use syn;

use quote::ToTokens;


/**
column 字段定义，json格式  {"name":"id","types":"uint16","size":1,"nullable":0,"is_primary":0,"is_defualt":0,"crud_value":"","precision":0,"comment":""}
column 字段名字是列名
text_t type;		//列字段类型
uint16    size;		//列宽
bool32    nullable;	//是否可空  1是0否
bool32 is_primary	//是否主键字段  1是0否
bool32 is_defualt	//是否有默认值  1是0否
text_t default_val	//默认值
text_t crud_value		//列值（查询、插入、修改时用到）
uint16 precision	//精度（即小数点后的位数）
text_t comment	//备注

index  字段定义，json格式  {"name":"id","cols":["id"],"col_count":1,"is_unique":0,"is_primary":0}
text_t  name;			//索引名
text_t *cols;			//索引列（可多个）
uint32  col_count;		//索引包含列的数量
bool32  is_unique;		//是否唯一  1是0否
bool32 is_primary	//是主键索引   1是0否
 */
#[proc_macro_derive(Create, attributes(column, index))]
pub fn create_macro(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();
    create_macro_impl(&ast)
}



fn create_macro_impl(ast: &syn::DeriveInput) -> TokenStream {
    //结构体名
    let name = &ast.ident;
    let mut column_count_quote = quote! {};

    let table_name_quote = quote! {
        let  table_name = codegen::utils::string_util::to_snake_name(stringify!(#name));
    };

    let mut column_count: u16 = 0;

    let mut column_vec : Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
    let mut index_vec : Vec<codegen::utils::model::StIndexDef> = Vec::new();
    match &ast.data {
        syn::Data::Struct(ds) => match &ds.fields {
            syn::Fields::Named(ff) => {
                let mut num: u16 = 0;

                ff.named.iter().for_each(|f| {
                    // let ident = &f.ident;
                    // let mut attr = String::from("");
                    // match ident {
                    //     Some(i) => attr = i.to_string(),
                    //     _ => {}
                    // };
                    // println!("attr: {}", attr);
                    column_count = column_count + 1;
                    column_count_quote = quote! {let mut column_count:u16 = #column_count;};
                    // let ty = &f.ty;
                    let attrs = &f.attrs;

                    // let mut ft = String::from("");
                    // match ty {
                    //     syn::Type::Path(p) => {
                    //         ft = format!("{}", p.path.segments[0].ident);
                    //     }
                    //     _ => {}
                    // }
                    // println!("type:{}", ft);

                    attrs.iter().for_each(|at| {
                        let mut prop = String::from("");
                        let mut param = String::from("");
                        if !at.path.segments.is_empty() && at.path.segments.len() > 0 {
                            prop = format!("{}", at.path.segments[0].ident);
                        }
                        if !at.tokens.is_empty() {
                            param = format!("{}", at.tokens);
                        }
                        if prop == "column".to_string() && param.len() > 0 {
                            let  column_string=     String::from(param).replace("(r", "").replace("#\"", "").replace("\"#", "").replace(")", "");
                            let column_def: codegen::utils::model::StColumnDef=serde_json::from_str(&column_string).unwrap();
                            let   s4c=codegen::utils::model::StColumnDef4C {                                                        
                                 name: column_def.name,
                                types:column_def.types,
                                col_slot: column_def.col_slot,
                                size:  column_def.size,
                                nullable: column_def.nullable,
                                is_primary:  column_def.is_primary,
                                is_default:  column_def.is_default,
                                default_val:  column_def.default_val,
                                crud_value:  column_def.crud_value,
                                precision:  column_def.precision,
                                comment:  column_def.comment,
                                assign_type_t: 0
                            };
                            column_vec.push(s4c);
                        } else if prop == "index".to_string() && param.len() > 0 {
                            let  index_string=     String::from(param).replace("(r", "").replace("#\"", "").replace("\"#", "").replace(")", "");
                            let index_model: codegen::utils::model::StIndexDef = serde_json::from_str(&index_string).unwrap();
                            index_vec.push( index_model);
                        };
                    });



                    num = num + 1;
                });

            }
            _ => {}
        },
        _ => {}
    }

    let index_vec_str = serde_json::to_string(&index_vec).unwrap();
    let column_vec_str = serde_json::to_string(&column_vec).unwrap();
    let create_table_column_quote= quote! {let  column_vec_strs=#column_vec_str;};
    let create_table_index_quote= quote! {let  index_vec_strs=#index_vec_str;};
    TokenStream::from(quote! {
        impl Create
         for #name {
            //创建表
            fn create_table( &self, conn: & interface::Interface,handle: u64)   -> std::result::Result<i32, codegen::utils::error::Error>  {
                use std::collections::HashMap;
                let mut columns : Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
                let mut indexs : Vec<codegen::utils::model::StIndexDef> = Vec::new();
                //表名
                #table_name_quote
                //字段
                 #create_table_column_quote
                 columns = serde_json::from_str(&column_vec_strs).unwrap();
                //索引
                #create_table_index_quote
                indexs= serde_json::from_str(&index_vec_strs).unwrap();
                let mut primary_map:HashMap<String, codegen::utils::model::StColumnDef4C> = HashMap::new();
                for c in &columns.clone() {
                    if(c.is_primary==1){
                        primary_map.insert(c.name.clone(),c.clone());
                    };
                }
                //创建表至少有一个索引
                // if(indexs.len()<1){
                //     panic !("Configure at least one index  indexs.len:{}",indexs.len());
                // }
                //创建表至少有一个主键索引  column.is_primary=1,index.is_unique,is_primary=1
                let mut b=false;
                for index in &indexs {
                    if(index.is_primary==1 && index.is_unique==1  &&  primary_map.contains_key(&index.name)){
                        b=true;
                    }
                }
                // if(!b){
                //     panic !("At least one primary key index is configured  indexs.len:{}",indexs.len());
                // }
                //创建表
                let  ret= conn.create_table_without_index(handle,&table_name, &columns);
                //创建索引
                let mut rets:i32=0;
                for index in &indexs {
                    let  ret= conn.create_table_index(handle,&table_name,&index);
                     println!("create_table_index = {:?}", ret);
                    if(ret !=0){
                         rets=-1;
                    }
                }
	            Ok(ret)
            }

            fn get_table_colums_info(&self) -> Vec<codegen::utils::model::StColumnDef4C> {
               #create_table_column_quote
                  //字段
               let mut colums : Vec<codegen::utils::model::StColumnDef4C>  = serde_json::from_str(&column_vec_strs).unwrap();
               colums
            }
           fn get_table_colums_info2(&self) -> Vec<interface::data::exp_column_def_t> {
               let mut columns2 : Vec<interface::data::exp_column_def_t> = Vec::new();
                #create_table_column_quote
                let mut colums : Vec<codegen::utils::model::StColumnDef4C>  = serde_json::from_str(&column_vec_strs).unwrap();
                for stColumnDef4C in colums{
                     let  mut s4c2=interface::data::exp_column_def_t {
                   name: interface::data::col_text_t {
                        str:  std::ffi::CString::new(stColumnDef4C.name.clone()).unwrap().into_raw(),
                        len:  stColumnDef4C.name.clone().len() as u32,
                        assign: interface::data::assign_type_t::from(0), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    },
                    col_type: interface::data::gs_type_t::from(&stColumnDef4C.types), //get_column_type(&column.types), //gs_type_t::GS_TYPE_VARCHAR,
                    col_slot: stColumnDef4C.col_slot,
                    size: stColumnDef4C.size,
                    nullable: stColumnDef4C.nullable as u32,
                    is_primary: stColumnDef4C.is_primary as u32,
                    is_default: stColumnDef4C.is_default as u32,
                    default_val: interface::data::col_text_t {
                        str:  std::ffi::CString::new(stColumnDef4C.default_val.clone()).unwrap().into_raw(),
                        len: stColumnDef4C.default_val.clone().len() as u32,
                        assign: interface::data::assign_type_t::from(0), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    },
                    crud_value: interface::data::col_text_t {
                        str:  std::ffi::CString::new(stColumnDef4C.crud_value.clone()).unwrap().into_raw(),
                        len:  stColumnDef4C.crud_value.clone().len() as u32,
                        assign: interface::data::assign_type_t::from(0), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    },
                    precision: stColumnDef4C.precision ,
                    comment: interface::data::col_text_t {
                        str:  std::ffi::CString::new(stColumnDef4C.comment.clone()).unwrap().into_raw(),
                        len: stColumnDef4C.comment.clone().len() as u32,
                        assign: interface::data::assign_type_t::from(0), //assign_type_t::ASSIGN_TYPE_EQUAL,
                    },

                    };

                   columns2.push(s4c2);
                }
                columns2
            }
            fn get_table_indexs(&self) -> Vec<codegen::utils::model::StIndexDef> {
                let mut indexs : Vec<codegen::utils::model::StIndexDef> = Vec::new();
                //索引
                #create_table_index_quote
                indexs= serde_json::from_str(&index_vec_strs).unwrap();
                indexs
            }


            //创建索引
            fn create_index( &self, conn:  & interface::Interface,handle: u64) ->std::result::Result<i32, codegen::utils::error::Error>  {
                let mut indexs : Vec<codegen::utils::model::StIndexDef> = Vec::new();
                //表名
                #table_name_quote
                //索引
                #create_table_index_quote
                indexs= serde_json::from_str(&index_vec_strs).unwrap();
                let mut rets:i32=0;
                for index in &indexs {
                    let  ret= conn.create_table_index(handle,&table_name,&index.clone());
                    if(ret !=0){
                         rets=-1;
                    }
                }
	             Ok(rets)
            }

        }
    })
}

#[proc_macro_attribute]
pub fn insert_macro_attribute(_args: TokenStream, _func: TokenStream) -> TokenStream {
    // 通过syn来解析输入的TokenStream
    let input_fn = syn::parse_macro_input!(_func as syn::ItemFn);
    // 获取该token对应的类型：ident
    // let name = input_fn.sig.ident.clone();
    let func_args_stream = input_fn.sig.inputs.to_token_stream();
    let st_column_def_quote = quote! {
	        let mut stColumnDef = codegen::utils::model::StColumnDef::default ();
	        stColumnDef.name = km.as_str().unwrap().to_string();
            if vm.is_str(){
             stColumnDef.crud_value = vm.as_str().unwrap().to_string();
            }else{
             stColumnDef.crud_value = vm.to_string().to_string();
            }
	        columns.push(stColumnDef);
	    };
    let insert_quote = quote! {
	        pub  fn insert_macro(
	            #func_args_stream
	        ) -> std::result::Result < i32, codegen::utils::error::Error > {
            	use std::collections::HashMap;
	            use serde::{Serialize, Deserialize};

                //获取table字段属性
                 let colums_vec=table.get_table_colums_info();
                 let mut colums_map:HashMap<String, codegen::utils::model::StColumnDef4C> = HashMap::new();
                 for colum in colums_vec{
                    colums_map.insert(colum.name.clone(),colum.clone());
                 }

	            let mut table_vec = Vec::new();
	            table_vec.push(table);
                let mut arr = rbs::to_value(table_vec).unwrap_or_default();
	            let mut columns: Vec < codegen::utils::model::StColumnDef > = Vec::new();
                //获取属性名，属性值
	            for (k, v) in arr{
	                for (km, vm) in & v {  //km字段名，vm字段值
	                  #st_column_def_quote
	                }
	            }
               // 重新组装insert字段集合
               // insert字段集合
                let mut insertVec : Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
                for column in columns {
                    let mut column_option = colums_map.get(&column.name.clone());
                    match column_option {
                        Some(col_opt) =>{
                            let mut col = ( * col_opt).clone();
                            col.name = column.name.clone();
                            col.crud_value = column.crud_value.clone();
                            insertVec.push(col);
                        },
                        None =>panic !("in columns no found field {}",&column.name.clone().to_string())
                    } ;
                 }

                let ret=  conn.intert_table_rows(handle,&table_name,&insertVec);
                // println!("********* intert_table_rows*************{}",ret);
                if(commit){
                    if(ret==0){
                        conn.db_commit(handle);
                    }else{
                       conn.db_rollback(handle);
                    }
                }
	            Ok(ret)
	        };
	   };
    return insert_quote.into();
}

#[proc_macro_attribute]
pub fn delete_macro_attribute(_args: TokenStream, _func: TokenStream) -> TokenStream {
    // 通过syn来解析输入的TokenStream
    let input_fn = syn::parse_macro_input!(_func as syn::ItemFn);
    // 获取该token对应的类型：ident
    // let name = input_fn.sig.ident.clone();
    let func_args_stream = input_fn.sig.inputs.to_token_stream();
    let delete_quote = quote! {
	        pub  fn delete_macro(
	            #func_args_stream
	        ) -> std::result::Result < i32, codegen::utils::error::Error > {
                 use std::collections::HashMap;
                let mut colums_map:HashMap < String, codegen::utils::model::StColumnDef4C > = HashMap::new ();
                for colum in colums_vec {
                    colums_map.insert(colum.name.clone(), colum.clone());
                }
                let mut restriction_vec:Vec < codegen::utils::model::StColumnDef4C > = Vec::new ();
                for column in restrictions {
                    let column_option = colums_map.get( & column.name.clone());
                    match column_option {
                        Some(col_opt) =>{
                            let mut col = ( * col_opt).clone();
                            col.name = column.name.clone();
                            col.crud_value = column.value.clone();
                            col.assign_type_t = column.assign.clone();
                            restriction_vec.push(col);
                        },
                        None =>panic !("in restrictions no found field {}",&column.name.clone().to_string())
                    } ;
                }
                let mut del_count:i32 = 0;
                let ret=conn.table_delete_row( handle,& table_name,&mut del_count,&restriction_vec);
                if(commit){
                    if(ret<0){
                     conn.db_rollback(handle);
                    // println!("*********delete rollback*************");
                    }else{
                      conn.db_commit(handle);
                     // println!("*********delete commit*************");
                    }
                }
                Ok(ret)
	        };
	   };
    return delete_quote.into();
}

#[proc_macro_attribute]
pub fn update_macro_attribute(_args: TokenStream, _func: TokenStream) -> TokenStream {
    // 通过syn来解析输入的TokenStream
    let input_fn = syn::parse_macro_input!(_func as syn::ItemFn);
    // 获取该token对应的类型：ident
    // let name = input_fn.sig.ident.clone();
    let func_args_stream = input_fn.sig.inputs.to_token_stream();
    let st_column_def_quote = quote! {
	        let mut stColumnDef = codegen::utils::model::StColumnDef::default ();
	        if vm.is_str(){
             stColumnDef.name = km.as_str().unwrap().to_string();
             stColumnDef.crud_value = vm.as_str().unwrap().to_string();
             columns.push(stColumnDef);
            }else  if !vm.is_null(){//非None,非字符串
             stColumnDef.name = km.as_str().unwrap().to_string();
             stColumnDef.crud_value = vm.to_string().to_string();
             columns.push(stColumnDef);
            }

	    };
    let update_quote = quote! {
	        pub  fn update_macro(
	            #func_args_stream
	        ) -> std::result::Result < i32, codegen::utils::error::Error > {
            	use std::collections::HashMap;
	            use serde::{Serialize, Deserialize};
                 let mut colums_map:HashMap<String, codegen::utils::model::StColumnDef4C> = HashMap::new();
                 for colum in colums_vec{
                    colums_map.insert(colum.name.clone(),colum.clone());
                 }
	            let mut table_vec = Vec::new();
	            table_vec.push(table);
                let mut arr = rbs::to_value(table_vec).unwrap_or_default();
	            let mut columns: Vec < codegen::utils::model::StColumnDef > = Vec::new();
                //获取属性名，属性值
	            for (k, v) in arr{
	                for (km, vm) in & v {  //km字段名，vm字段值
	                  #st_column_def_quote
	                }
	            }
                //重新组装更新字段集合
                //update字段集合
                let mut updateVec : Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
                //条件字段集合
                let mut restrictionVec : Vec<codegen::utils::model::StColumnDef4C> = Vec::new();
                for column in columns {
                    let mut column_option = colums_map.get(&column.name.clone());
                    match column_option {
                        Some(col_opt) =>{
                            let mut col = ( * col_opt).clone();
                            col.name = column.name.clone();
                            col.crud_value = column.crud_value.clone();
                            updateVec.push(col);
                        },
                        None =>panic !("in columns no found field {}",&column.name.clone().to_string())
                    } ;
                 }

                for column in restrictions {
                    let mut column_option = colums_map.get(&column.name.clone());
                    match column_option {
                        Some(col_opt) =>{
                            let mut col = ( * col_opt).clone();
                            col.name = column.name.clone();
                            col.crud_value = column.value.clone();
                            col.assign_type_t = column.assign.clone();
                            restrictionVec.push(col);
                        },
                        None =>panic !("in restrictions no found field {}",&column.name.clone().to_string())
                    } ;
                 }
                let ret=  conn.update_table_rows(handle,&table_name,&updateVec,&restrictionVec);
                if(commit){
                    if(ret==0){
                       conn.db_commit(handle);
                    }else{
                       conn.db_rollback(handle);
                    }
                }
                //println!("update_table_rows ret**********: {}", ret.to_string());
	            Ok(ret)
	        };
	   };
    return update_quote.into();
}
