#[macro_export]
macro_rules! crud {
    ($table:ty{}) => {
        $crate::impl_insert!($table {});
        $crate::impl_delete!($table {});
        $crate::impl_update!($table {});
        $crate::impl_select!($table {});
        $crate::impl_commit!($table {});
    };
    ($table:ty{},$table_name:expr) => {
        $crate::impl_insert!($table {}, $table_name);
        $crate::impl_delete!($table {}, $table_name);
        $crate::impl_update!($table {}, $table_name);
        $crate::impl_commit!($table {}, $table_name);
    };
}


#[macro_export]
macro_rules! impl_commit
{
    ($table:ty{}) => {
        $crate::impl_commit!(
            $table {},
            codegen::utils::string_util::to_snake_name(stringify!($table))
        );
    };

     ($table:ty{},$table_name:expr) => {
        impl $table {
            pub  fn commit(
                conn: &interface::Interface,
                handle: u64
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                conn.db_commit(handle);
                Ok(0)
            }

            pub  fn rollback(
                conn: &interface::Interface,
                handle: u64
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                conn.db_rollback(handle);
                  Ok(0)
            }

            pub  fn db_shutdown(
                conn: &interface::Interface,
                handle: u64
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                conn.db_shutdown();
                  Ok(0)
            }
        }
    };
}

#[macro_export]
macro_rules! impl_insert
{
    ($table:ty{}) => {
        $crate::impl_insert!(
            $table {},
            codegen::utils::string_util::to_snake_name(stringify!($table))
        );
    };
    ($table:ty{},$table_name:expr) => {
        impl $table {
            //insert and commit
            pub  fn insert_commit(
                conn: &interface::Interface,
                handle: u64,
                table: &$table
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                #[macro_driver::insert_macro_attribute("")]
                  pub   fn insert_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table: &$table,
                    table_name: String,
                    commit: bool,
                ) -> std::result::Result<i32, codegen::utils::error::Error>
                {
                    impled!()
                }
                //表名
                let mut  table_name = $table_name.to_string();
                if table_name.is_empty(){
                  table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
                }
                insert_macro(conn,handle,&table,table_name,true)
            }

            //insert no commit
            pub  fn insert(
                conn: &interface::Interface,
                handle: u64,
                table: &$table
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                #[macro_driver::insert_macro_attribute("")]
                  pub   fn insert_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table: &$table,
                    table_name: String,
                    commit: bool,
                ) -> std::result::Result<i32, codegen::utils::error::Error>
                {
                    impled!()
                }
                //表名
                let mut  table_name = $table_name.to_string();
                if table_name.is_empty(){
                  table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
                }
                insert_macro(conn,handle,&table,table_name,false)
            }

        }
    };
}

#[macro_export]macro_rules! impl_delete
{
($table:ty {}) =>{
      $crate::impl_delete !(
            $table {},
            codegen::utils::string_util::to_snake_name (stringify !($table))
            );
        } ;
($table:ty {},$table_name:expr) =>{
            impl $table {
            //单个条件删除
            pub  fn delete_by_column_commit (
                conn: &interface::Interface,
                handle: u64,
                restriction:codegen::utils::model::Restriction
            )  ->std::result::Result < i32, codegen::utils::error::Error > {
                 #[macro_driver::delete_macro_attribute("")]
                 fn delete_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table_name: String,
                    restrictions:Vec<codegen::utils::model::Restriction>,
                    colums_vec:Vec<codegen::utils::model::StColumnDef4C>,
                    commit: bool,
                ) -> std::result::Result<codegen::utils::db::ExecResult, codegen::utils::error::Error>
                {
                    impled!()
                }
                //获取table字段属性
                let colums_vec =<$table >::default().get_table_colums_info();
                //表名
                let mut table_name = $table_name.to_string();
                if table_name.is_empty() {
                    table_name = codegen::utils::string_util::to_snake_name (stringify !($table));
                }
                let mut restrictions:Vec < codegen::utils::model::Restriction > = Vec::new ();
                 restrictions.push(restriction);
                 delete_macro(conn,handle,table_name,restrictions,colums_vec,true)
            }


                //单个条件删除
            pub  fn delete_by_column (
                conn: &interface::Interface,
                handle: u64,
                restriction:codegen::utils::model::Restriction
            )  ->std::result::Result < i32, codegen::utils::error::Error > {
                 #[macro_driver::delete_macro_attribute("")]
                 fn delete_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table_name: String,
                    restrictions:Vec<codegen::utils::model::Restriction>,
                    colums_vec:Vec<codegen::utils::model::StColumnDef4C>,
                    commit: bool,
                ) -> std::result::Result<codegen::utils::db::ExecResult, codegen::utils::error::Error>
                {
                    impled!()
                }
                //获取table字段属性
                let colums_vec =<$table >::default().get_table_colums_info();
                //表名
                let mut table_name = $table_name.to_string();
                if table_name.is_empty() {
                    table_name = codegen::utils::string_util::to_snake_name (stringify !($table));
                }
                let mut restrictions:Vec < codegen::utils::model::Restriction > = Vec::new ();
                 restrictions.push(restriction);
                 delete_macro(conn,handle,table_name,restrictions,colums_vec,false)
            }

           }
     } ;
}



#[macro_export]
macro_rules! impl_update
{
    ($table:ty{}) => {
        $crate::impl_update!(
            $table {},
            codegen::utils::string_util::to_snake_name(stringify!($table))
        );
    };
    ($table:ty{},$table_name:expr) => {
        impl $table {

            pub  fn update_by_column_commit(
                conn: &interface::Interface,
                handle: u64,
                table: &$table,
                restriction: codegen::utils::model::Restriction
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                #[macro_driver::update_macro_attribute("")]
                 fn update_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table: &$table,
                    table_name: String,
                    restrictions:Vec<codegen::utils::model::Restriction>,
                    colums_vec:Vec<codegen::utils::model::StColumnDef4C>,
                    commit: bool,
                ) -> std::result::Result<codegen::utils::db::ExecResult, codegen::utils::error::Error>
                {
                    impled!()
                }
                //表名
                let mut  table_name = $table_name.to_string();
                if table_name.is_empty(){
                  table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
                }
                //获取table字段属性
                let colums_vec=table.get_table_colums_info();
                let mut restrictions:Vec<Restriction>=Vec::new();
                restrictions.push(restriction);
                update_macro(conn,handle,table,table_name,restrictions,colums_vec,true)
            }


            pub  fn update_by_column(
                conn: &interface::Interface,
                handle: u64,
                table: &$table,
                restriction: codegen::utils::model::Restriction
            ) -> std::result::Result<i32, codegen::utils::error::Error> {
                #[macro_driver::update_macro_attribute("")]
                 fn update_macro(
                    conn: &interface::Interface,
                    handle: u64,
                    table: &$table,
                    table_name: String,
                    restrictions:Vec<codegen::utils::model::Restriction>,
                    colums_vec:Vec<codegen::utils::model::StColumnDef4C>,
                    commit: bool,
                ) -> std::result::Result<codegen::utils::db::ExecResult, codegen::utils::error::Error>
                {
                    impled!()
                }
                //表名
                let mut  table_name = $table_name.to_string();
                if table_name.is_empty(){
                  table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
                }
                //获取table字段属性
                let colums_vec=table.get_table_colums_info();
                let mut restrictions:Vec<Restriction>=Vec::new();
                restrictions.push(restriction);
                update_macro(conn,handle,table,table_name,restrictions,colums_vec,false)
            }


        }
    };
}



#[macro_export]macro_rules!
impl_select
{
     ($table:ty{}) => {
        $crate::impl_select!(
            $table {},
            codegen::utils::string_util::to_snake_name(stringify!($table))
        );
    };
    ($table:ty{},$table_name:expr) => {
        impl $table {
            //单个条件查询
            pub  fn select_by_column(
                conn: &interface::Interface,
                handle: u64,
                restriction: codegen::utils::model::Restriction,
          )  -> std::result::Result<Vec<$table>,  codegen::utils::error::Error> {
              use std::collections::HashMap;
                 //获取table字段属性
              let colums_vec: Vec<interface::data::exp_column_def_t> =<$table>::default().get_table_colums_info2();
              let colums_vec_ref=&colums_vec;
              let  indexs_vec: Vec<codegen::utils::model::StIndexDef>= <$table>::default().get_table_indexs();
              let mut colums_map:HashMap<String, interface::data::exp_column_def_t> = HashMap::new();
              for colum in &*colums_vec_ref{
                  let  name=unsafe {std::ffi::CStr::from_ptr(colum.name.str).to_str().unwrap()};
                  colums_map.insert(name.to_string(),*colum);
              }
              //条件字段集合
              let mut restriction_vec : Vec<interface::data::exp_column_def_t> = Vec::new();
              let restriction_ref=&restriction;
              let  column_option = colums_map.get(&*restriction_ref.name);
              match column_option {
                      Some(col_opt) =>{
                          let mut col = ( * col_opt).clone();
                          col.crud_value.str =std::ffi::CString::new(&*restriction_ref.value).unwrap().into_raw();
                          col.crud_value.len =(&*restriction_ref.value).len() as u32;
                          col.crud_value.assign =interface::data::assign_type_t::from( *&restriction_ref.assign);
                          restriction_vec.push(col);
                      },
                      None =>panic !("in restriction no found field {}",&*restriction_ref.name)
                  } ;
              //表名
              let mut table_name = $table_name.to_string();
              if table_name.is_empty(){
                table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
              }
              let mut vv : Vec<serde_json::value::Value> = Vec::new();
              conn.table_fetch_data(handle,&table_name, &restriction_vec,colums_vec,&indexs_vec,&mut vv);
              Ok( serde_json::from_value::<Vec<$table>>(serde_json::Value::Array(vv)).unwrap())

          }
            //多个条件
            pub  fn select_by_columns(
                 conn: &interface::Interface,
                 handle: u64,
                 restrictions: Vec<Restriction>,
            )  ->std::result::Result<Vec<$table>,  codegen::utils::error::Error> {
                 if restrictions.len()<1{
                    return Err(codegen::utils::error::Error::from("Vec can't be empty!"));
                 }
                 use std::collections::HashMap;
                 //获取table字段属性
              let colums_vec: Vec<interface::data::exp_column_def_t> =<$table>::default().get_table_colums_info2();
               let colums_vec_ref=&colums_vec;
              let  indexs_vec: Vec<codegen::utils::model::StIndexDef>= <$table>::default().get_table_indexs();
              let mut colums_map:HashMap<String, interface::data::exp_column_def_t> = HashMap::new();
              for colum in &*colums_vec_ref{
                  let  name=unsafe {std::ffi::CStr::from_ptr(colum.name.str).to_str().unwrap()};
                  colums_map.insert(name.to_string(),*colum);
              }

              let mut restriction_vec : Vec<interface::data::exp_column_def_t> = Vec::new();

              for r in restrictions {
                      let restriction_ref=&r;
                      let  column_option = colums_map.get(&*restriction_ref.name);
                      match column_option {
                          Some(col_opt) =>{
                              let mut col = ( * col_opt).clone();
                              col.crud_value.str =std::ffi::CString::new(&*restriction_ref.value).unwrap().into_raw();
                              col.crud_value.len =(&*restriction_ref.value).len() as u32;
                              col.crud_value.assign =interface::data::assign_type_t::from( *&restriction_ref.assign);
                              restriction_vec.push(col);
                          },
                          None =>panic !("in restriction no found field {}",&*restriction_ref.name)
                      } ;
                }
                //表名
                let mut table_name = $table_name.to_string();
                if table_name.is_empty(){
                  table_name =  codegen::utils::string_util::to_snake_name(stringify!($table));
                }
                let mut vv : Vec<serde_json::value::Value> = Vec::new();
                conn.table_fetch_data(handle,&table_name, &restriction_vec,colums_vec,&indexs_vec,&mut vv);
                Ok( serde_json::from_value::<Vec<$table>>(serde_json::Value::Array(vv)).unwrap())

            }
        }
    };

}


#[macro_export]
macro_rules! impled {
    () => {
        unimplemented!()
    };
}
