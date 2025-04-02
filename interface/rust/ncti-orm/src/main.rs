mod crud;
use codegen::utils::model::Restriction;
use interface;
use interface::data::dbtype_t;
use interface::Interface;
use macro_driver;
use macro_driver::Create;
use ncti_orm::trails::Create;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io;
use std::env;

#[derive(Debug, Clone, Create, Serialize, Deserialize, Default)]
pub struct User {
    #[column(r#"{"name":"id","types":"VARCHAR","col_slot":0,"size":10,"nullable":0,"is_primary":1,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
    #[index(r#"{"name":"IDX_id","cols":["id"],"idx_slot":0,"col_count":1,"is_unique":1,"is_primary":1}"#)]
    id: Option<String>,
    #[column(r#"{"name":"name1","types":"VARCHAR","col_slot":1,"size":10,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
    //#[index(r#"{"name":"IDX_name1","cols":["name1"],"idx_slot":1,"col_count":1,"is_unique":0,"is_primary":0}"#)]
    name1: Option<String>,
    #[column(r#"{"name":"name2","types":"VARCHAR","col_slot":2,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
    name2: Option<String>,
    #[column(r#"{"name":"name3","types":"VARCHAR","col_slot":3,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
    name3: Option<String>,
    #[column(r#"{"name":"name4","types":"VARCHAR","col_slot":4,"size":40,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
    name4: Option<String>,
}

crud!(User {});

pub fn main() {

 

    show();
    let mut s = String::new();
    io::stdin().read_line(&mut s)   // 输入一行字符串
        .expect("failed to read line.");
     let  mut b=true;


     while b {
        let mut d=s.clone();
        match  d.trim().to_string().as_str() {
            "1" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                println!("创建表中...");
                rdb_001_create_table();
                println!("创建表结束");
                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
               
            }
            "2" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
              
                rdb_001_insert();

                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "3" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                
                rdb_003_insert();
                rdb_003_select();

                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "4" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
              
                rdb_004_select();
                rdb_004_update();

                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "5" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
               
                rdb_005_insert();
                rdb_005_delete();

                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "6" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                
                rdb_008_insert();
                rdb_008_commit();
               
                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "7" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                
                rdb_007_insert();
                rdb_007_rollback();
               
                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "8" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                
               
                rdb_index_001();
               
                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }
             "9" => { 
                s.clear();
                std::process::Command::new("clear").status().unwrap();
                
              
                rdb_index_002();
               
                show();
                io::stdin().read_line(&mut s)   // 输入一行字符串
                .expect("failed to read line.");
             }

             
             _ => {
              s.clear();
              println!("退出");
              b=false;
              }

          }
    }

    fn show() {
        println!("");
        println!("请输入下面数字功能：");
        println!("1.关系型数据库_创建表");
        println!("2.关系型数据库_新增方法");
        println!("3.关系型数据库_查询方法");
        println!("4.关系型数据库_修改方法");
        println!("5.关系型数据库_删除方法");
        println!("6.关系型数据库_事务提交");
        println!("7.关系型数据库_事务回滚");
        println!("8.关系型数据库_创建索引index_001");
        println!("9.关系型数据库_创建索引index_002");
        println!("0.退出");
    }

}
//************************************  用例rdb-001：创建表  ***************************************/
    //1，创建表方法
    fn get_project_path() -> String {
        if let Ok(path) = env::current_dir() {
            if let Some(str_path) = path.to_str() {
                return str_path.to_string();
            }
        }
        "/home/data".to_string()
    }
    
    fn rdb_001_create_table() {
        let data_path =get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let create_table_ret = User::default().create_table(&conn, handle);
        println!("create_table_ret = {:?}", create_table_ret);
        conn.db_shutdown();
    }

    //2，新增方法
    
    fn rdb_001_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("1".to_string()),
            name1: Some("1".to_string()),
            name2: Some("1".to_string()),
            name3: Some("1".to_string()),
            name4: Some("1".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        println!("insert_result = {:?}", insert_result);

        let r = Restriction::equ("id".to_string(), "1".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        conn.db_shutdown();
        println!("select_result = {:?}", data);
    }

    //************************************  用例rdb-002：基础操作_新增方法  ***************************************/
    //1，新增方法
    
    fn rdb_002_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("2".to_string()),
            name1: Some("2".to_string()),
            name2: Some("2".to_string()),
            name3: Some("2".to_string()),
            name4: Some("2".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        println!("insert_result = {:?}", insert_result);

        let r = Restriction::equ("id".to_string(), "2".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("select_result = {:?}", data);
        conn.db_shutdown();
        
    }

    //************************************  用例rdb-003：基础操作_查询方法  ***************************************/
    //1，新增方法（用例rdb-003）
    
    fn rdb_003_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("3".to_string()),
            name1: Some("3".to_string()),
            name2: Some("3".to_string()),
            name3: Some("3".to_string()),
            name4: Some("3".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        println!("insert_result = {:?}", insert_result);

        conn.db_shutdown();

    }

    //2，查询方法
    
    fn rdb_003_select() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let r = Restriction::equ("id".to_string(), "3".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("select_result = {:?}", data);
        conn.db_shutdown();
    }

    //************************************  用例rdb-004：基础操作_修改方法  ***************************************/
    //1，查询方法
    
    fn rdb_004_select() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let r = Restriction::equ("id".to_string(), "1".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("select_result = {:?}", data);
        conn.db_shutdown();
    }

    //2，修改方法
    
    fn rdb_004_update() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let r = Restriction::equ("id".to_string(), "1".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("修改前的数据 = {:?}", data);

        let t = User {
            id: Some("1".to_string()),
            name1: Some("update1".to_string()),
            name2: Some("update2".to_string()),
            name3: Some("update3".to_string()),
            name4: Some("update4".to_string()),
        };

        let update_result = User::update_by_column_commit(&conn, handle, &t, r.clone());
        println!("update_result = {:?}", update_result);

        let r = Restriction::equ("id".to_string(), "1".to_string());
        let data = User::select_by_column(&conn, handle, r.clone());
        println!("修改后数据 = {:?}", data);
        conn.db_shutdown();
    }

    //************************************  用例rdb-005：基础操作_删除方法  ***************************************/
    //1，新增方法
    
    fn rdb_005_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("5".to_string()),
            name1: Some("5".to_string()),
            name2: Some("5".to_string()),
            name3: Some("5".to_string()),
            name4: Some("5".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        println!("insert_result = {:?}", insert_result);
        conn.db_shutdown();
    }

    //2，删除方法
    
    fn rdb_005_delete() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let  conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let  handle: u64 = 0;
        conn.init_handle(&handle);

        let r = Restriction::equ("id".to_string(), "5".to_string());
        let data = User::select_by_column(&conn, handle, r.clone());
        println!("删除前id为5的数据 = {:?}", data);
        let commit_result = User::delete_by_column_commit(&conn, handle, r.clone());
        println!("commit_result = {:?}", commit_result);
        let data = User::select_by_column(&conn, handle, r.clone());
        println!("删除后id为5的数据 = {:?}", data);
        conn.db_shutdown();
    }

    //************************************  用例rdb-006：事务_单个操作回滚  ***************************************/
    //1，新增方法
    
    fn rdb_006_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("6".to_string()),
            name1: Some("6".to_string()),
            name2: Some("6".to_string()),
            name3: Some("6".to_string()),
            name4: Some("6".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        println!("insert_result = {:?}", insert_result);
        conn.db_shutdown();
    }

    //2，单个操作回滚方法
    
    fn rdb_006_rollback() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);

        let r = Restriction::equ("id".to_string(), "6".to_string());
        let data = User::select_by_column(&conn, handle, r.clone());
        println!("删除前id为6的数据 = {:?}", data);
        let ret = User::delete_by_column(&conn, handle, r.clone());
        println!("删除结果= {:?}", ret);
        let r1 = Restriction::equ("id".to_string(), "6".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("删除后id为6的数据(事务未提交commit) = {:?}", data);
        //回滚操作
        let rollback_result = User::rollback(&conn, handle);
        println!("rollback_result = {:?}", rollback_result);
        let r1 = Restriction::equ("id".to_string(), "6".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("事务回滚后id为6的数据 = {:?}", data);
        conn.db_shutdown();
    }

    //************************************  用例rdb-007：事务_多个操作回滚  ***************************************/
    //1，新增方法
    
    fn rdb_007_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("7".to_string()),
            name1: Some("7".to_string()),
            name2: Some("7".to_string()),
            name3: Some("7".to_string()),
            name4: Some("7".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        let r = Restriction::equ("id".to_string(), "7".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("select_result = {:?}", data);
        conn.db_shutdown();
    }

    //2，多个操作回滚方法
    
    fn rdb_007_rollback() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);

        //4，查询各id的回滚前的情况
        let r1 = Restriction::equ("id".to_string(), "7".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("id为7的数据 = {:?}", data);
        let r1 = Restriction::equ("id".to_string(), "8".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("id为8的数据 = {:?}", data);

        //2，删除方法（不自动提交事务）
        let r = Restriction::equ("id".to_string(), "7".to_string());
        let ret = User::delete_by_column(&conn, handle, r.clone());

        //3，新增方法（不自动提交事务）
        let t = User {
            id: Some("8".to_string()),
            name1: Some("8".to_string()),
            name2: Some("8".to_string()),
            name3: Some("8".to_string()),
            name4: Some("8".to_string()),
        };
        let insert_result = User::insert(&conn, handle, &t);

        //4，修改方法（不自动提交事务）
        let t = User {
            id: Some("8".to_string()),
            name1: Some("update8".to_string()),
            name2: Some("update8".to_string()),
            name3: Some("update8".to_string()),
            name4: Some("update8".to_string()),
        };
        let r = Restriction::equ("id".to_string(), "8".to_string());
        let update_result = User::update_by_column(&conn, handle, &t, r.clone());

        //4，查询各id的回滚前的情况
        let r1 = Restriction::equ("id".to_string(), "7".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("事务开启后，删除id为7的数据 = {:?}", data);
        let r1 = Restriction::equ("id".to_string(), "8".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("事务开启后，新增并修改id为8的数据 = {:?}", data);

        //5，回滚操作
        let rollback_result = User::rollback(&conn, handle);
        println!("rollback_result = {:?}", rollback_result);

        //6，查询各id提交事务后的执行情况
        let r1 = Restriction::equ("id".to_string(), "7".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("事务回滚后id为7的数据 = {:?}", data);
        let r2 = Restriction::equ("id".to_string(), "8".to_string());
        let data = User::select_by_column(&conn, handle, r2.clone());
        println!("事务回滚后id为8的数据 = {:?}", data);
        conn.db_shutdown();
    }

    //************************************  用例rdb-008：事务_commit提交  ***************************************/
    //1，新增方法
    
    fn rdb_008_insert() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        let t = User {
            id: Some("10".to_string()),
            name1: Some("10".to_string()),
            name2: Some("10".to_string()),
            name3: Some("10".to_string()),
            name4: Some("10".to_string()),
        };

        let insert_result = User::insert_commit(&conn, handle, &t);
        let r = Restriction::equ("id".to_string(), "10".to_string());

        let data = User::select_by_column(&conn, handle, r.clone());
        println!("select_result = {:?}", data);
        conn.db_shutdown();
    }

    //1，事务手动提交方法
    
    fn rdb_008_commit() {
        let data_path = get_project_path()+ &String::from("/output/data_add_thread1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        //1，查询各id的提交任务前的情况
        let r1 = Restriction::equ("id".to_string(), "10".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("id为10的数据 = {:?}", data);
        let r1 = Restriction::equ("id".to_string(), "11".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("id为11的数据 = {:?}", data);

        //3，删除方法（不自动提交事务）
        let r = Restriction::equ("id".to_string(), "10".to_string());
        let ret = User::delete_by_column(&conn, handle, r.clone());
        println!("delete_result= {:?}", ret);

        //4，新增方法（不自动提交事务）
        let t = User {
            id: Some("11".to_string()),
            name1: Some("11".to_string()),
            name2: Some("11".to_string()),
            name3: Some("11".to_string()),
            name4: Some("11".to_string()),
        };
        let insert_result = User::insert(&conn, handle, &t);

        //5，手动提交事务操作
        let commit_result = User::commit(&conn, handle);
        println!("commit_result = {:?}", commit_result);

        //6，查询各id提交事务后的执行情况
        let r1 = Restriction::equ("id".to_string(), "10".to_string());
        let data = User::select_by_column(&conn, handle, r1.clone());
        println!("事务提交后id为10的数据 = {:?}", data);
        let r2 = Restriction::equ("id".to_string(), "11".to_string());
        let data = User::select_by_column(&conn, handle, r2.clone());
        println!("事务提交后id为11的数据 = {:?}", data);
        conn.db_shutdown();
    }

    // ******************************************  创建索引测试 ******************************************

    //****************************  用例rdb_index_001：未创建索引 ******************************/
    
    fn rdb_index_001() {
        #[derive(Debug, Clone, Create, Serialize, Deserialize, Default)]
        pub struct User {
            #[column(r#"{"name":"id","types":"VARCHAR","col_slot":0,"size":10,"nullable":0,"is_primary":1,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            id: Option<String>,
            #[column(r#"{"name":"name1","types":"VARCHAR","col_slot":1,"size":10,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name1: Option<String>,
            #[column(r#"{"name":"name2","types":"VARCHAR","col_slot":2,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name2: Option<String>,
            #[column(r#"{"name":"name3","types":"VARCHAR","col_slot":3,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name3: Option<String>,
            #[column(r#"{"name":"name4","types":"VARCHAR","col_slot":4,"size":40,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name4: Option<String>,
        }

        crud!(User {});
        let data_path =get_project_path()+ &String::from("/output/data_p1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        println!("***********创建表*************");
        let create = User::default().create_table(&conn, handle);
        println!("create = {:?}", create);
        conn.db_shutdown();

        //重新打开
        let mut data_path =get_project_path()+ &String::from("/output/data_p1");
        println!("new =");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        println!("open_db =start");
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);

        //插入测试数据
        println!("***********开始插入测试数据*************");

        let mut i = 1;
        //  let  max=10000;
        while i < 1001 {
            let t = User {
                id: Some(i.to_string()),
                name1: Some(i.to_string()),
                name2: Some(i.to_string()),
                name3: Some(i.to_string()),
                name4: Some(i.to_string()),
            };

            let insert_result = User::insert(&conn, handle, &t);
            if (i % 100 == 0) {
                User::commit(&conn, handle);
            }
            i = i + 1;
        }

        //测试是否插入成功

        let num = 1000;
        let t1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("get_current_unix_err")
            .as_millis();
        for n in 1..num {
            let data = User::select_by_column(
                &conn,
                handle,
                Restriction::equ("id".to_string(), n.to_string()),
            );
            println!("查询出的数据： {:?}", data);
        }
        let t2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("get_current_unix_err")
            .as_millis();
        println!("结束时间={}", t2);

        println!(
            "查询总数={}，未创建索引的总花费时间（毫秒）={}",
            num,
            t2 - t1
        );
        conn.db_shutdown();
    }

    //****************************  用例rdb_index_002：创建索引 ******************************/
    
    fn rdb_index_002() {
        #[derive(Debug, Clone, Create, Serialize, Deserialize, Default)]
        pub struct User {
            #[column(r#"{"name":"id","types":"VARCHAR","col_slot":0,"size":10,"nullable":0,"is_primary":1,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            #[index(r#"{"name":"IDX_id","cols":["id"],"idx_slot":0,"col_count":1,"is_unique":1,"is_primary":1}"#)]
            id: Option<String>,
            #[column(r#"{"name":"name1","types":"VARCHAR","col_slot":1,"size":10,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name1: Option<String>,
            #[column(r#"{"name":"name2","types":"VARCHAR","col_slot":2,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name2: Option<String>,
            #[column(r#"{"name":"name3","types":"VARCHAR","col_slot":3,"size":20,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name3: Option<String>,
            #[column(r#"{"name":"name4","types":"VARCHAR","col_slot":4,"size":40,"nullable":0,"is_primary":0,"is_default":0,"default_val":"","crud_value":"","precision":0,"comment":""}"#)]
            name4: Option<String>,
        }

        crud!(User {});
        let data_path = get_project_path()+ &String::from("/output/data_p1");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);
        println!("***********创建表*************");
        let create = User::default().create_table(&conn, handle);
        println!("create = {:?}", create);
        conn.db_shutdown();

        //重新打开
        let mut data_path = get_project_path()+ &String::from("/output/data_p1");
        println!("new =");
        let mut conn = Interface::new(data_path, dbtype_t::DB_TYPE_GSTOR as libc::c_int);
        println!("open_db =start");
        conn.open_db();
        let mut handle: u64 = 0;
        conn.init_handle(&handle);

        //插入测试数据
        println!("***********开始插入测试数据*************");

        let mut i = 1;
        //  let  max=10000;
        while i < 1001 {
            let t = User {
                id: Some(i.to_string()),
                name1: Some(i.to_string()),
                name2: Some(i.to_string()),
                name3: Some(i.to_string()),
                name4: Some(i.to_string()),
            };

            let insert_result = User::insert(&conn, handle, &t);
            if (i % 100 == 0) {
                User::commit(&conn, handle);
            }
            i = i + 1;
        }

        //测试是否插入成功

        let num = 1000;
        let t1 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("get_current_unix_err")
            .as_millis();
        for n in 1..num {
            let data = User::select_by_column(
                &conn,
                handle,
                Restriction::equ("id".to_string(), n.to_string()),
            );
            println!("查询出的数据： {:?}", data);
        }
        let t2 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("get_current_unix_err")
            .as_millis();
        println!("结束时间={}", t2);
        println!("查询总数={}，创建索引的总花费时间（毫秒）={}", num, t2 - t1);
        conn.db_shutdown();
    }

//************************************************  功能测试  ***************************************************/

//************************************  用例RDB-001：创建表  ***************************************/
    //1，创建表方法
    #[test]
    fn RDB_001_create_table() {
        rdb_001_create_table();
    }

    //2，新增方法
    #[test]
    fn RDB_001_insert() {
        rdb_001_insert() ;
    }

    //************************************  用例RDB-002：基础操作_新增方法  ***************************************/
    //1，新增方法
    #[test]
    fn RDB_002_insert() {
        rdb_002_insert();
    }

    //************************************  用例RDB-003：基础操作_查询方法  ***************************************/
    //1，新增方法（用例RDB-003）
    #[test]
    fn RDB_003_insert() {
        rdb_003_insert();
    }

    //2，查询方法
    #[test]
    fn RDB_003_select() {
        rdb_003_select();
    }

    //************************************  用例RDB-004：基础操作_修改方法  ***************************************/
    //1，查询方法
    #[test]
    fn RDB_004_select() {
        rdb_004_select();
    }

    //2，修改方法
    #[test]
    fn RDB_004_update() {
       rdb_004_update();
    }

    //************************************  用例RDB-005：基础操作_删除方法  ***************************************/
    //1，新增方法
    #[test]
    fn RDB_005_insert() {
        rdb_005_insert();
    }

    //2，删除方法
    #[test]
    fn RDB_005_delete() {
        rdb_005_delete();
    }

    //************************************  用例RDB-006：事务_单个操作回滚  ***************************************/
    //1，新增方法
    #[test]
    fn RDB_006_insert() {
        rdb_006_insert();
    }

    //2，单个操作回滚方法
    #[test]
    fn RDB_006_rollback() {
        rdb_006_rollback();
    }

    //************************************  用例RDB-007：事务_多个操作回滚  ***************************************/
    //1，新增方法
    #[test]
    fn RDB_007_insert() {
        rdb_007_insert();
    }

    //2，多个操作回滚方法
    #[test]
    fn RDB_007_rollback() {
        rdb_007_rollback();
    }

    //************************************  用例RDB-008：事务_commit提交  ***************************************/
    //1，新增方法
    #[test]
    fn RDB_008_insert() {
        rdb_008_insert() ;
    }

    //1，事务手动提交方法
    #[test]
    fn RDB_008_commit() {
        rdb_008_commit();
    }

    // ******************************************  创建索引测试 ******************************************

    //****************************  用例RDB_index_001：未创建索引 ******************************/
    #[test]
    fn RDB_index_001() {
        rdb_index_001();
    }

    //****************************  用例RDB_index_002：创建索引 ******************************/
    #[test]
    fn RDB_index_002() {
       rdb_index_002();
    }


