extern crate libc;
use std::ffi::CString;
use std::ffi::CStr;
use libc::c_char;
use rand::distributions::{Alphanumeric,DistString};
use std::{thread, time, env, process};

#[link(name = "gstor")]

extern "C" {
    fn gstor_startup(data_path: CString) -> libc::c_int;
    fn gstor_set_param(name: *const c_char, value: *const c_char, data_path: *const c_char) -> libc::c_int;
    fn gstor_alloc(handle: *const u64) -> libc::c_int;
    fn gstor_open_table(handle: u64, table_name: CString) -> libc::c_int;
    fn gstor_put(handle: u64, key: *const c_char, key_len: libc::c_uint, val: *const c_char, val_len: libc::c_uint) -> libc::c_int;
    fn rust_gstor_get(handle: u64, key: *const c_char) -> *const c_char;
    fn gstor_commit(handle: u64) -> libc::c_int;
    fn gstor_shutdown();
}

fn main() {

    let args: Vec<String> = env::args().collect();
    // println!("{:?}", args);

    if args.len() != 3 {
        println!("Usage: cmd -D data_path");
        process::exit(-1);
    }

    let op = args.get(1);
    let param = args.get(2);

    if op.unwrap() != "-D" {
        println!("Usage: cmd -D data_path");
        process::exit(-1);
    }
    
    if param.unwrap().len() == 0 {
        println!("param error");
        process::exit(-1);
    }
    let data_path = CString::new(String::from(param.unwrap())).unwrap();
    let handle = Box::<u64>::new(0);
    
    /* 初始化参数 */
    let value = CString::new("134217728").unwrap();
    let name = CString::new("DATA_BUFFER_SIZE").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };

    let name = CString::new("BUF_POOL_NUM").unwrap();
    let value = CString::new("1").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };

    let name = CString::new("LOG_BUFFER_SIZE").unwrap();
    let value = CString::new("16777216").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };

    let name = CString::new("LOG_BUFFER_COUNT").unwrap();
    let value = CString::new("4").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };

    let name = CString::new("SPACE_SIZE").unwrap();
    let value = CString::new("134217728").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };

    let name = CString::new("LOG_LEVEL").unwrap();
    let value = CString::new("55").unwrap();
    let ret = unsafe {
        gstor_set_param(name.as_ptr(), value.as_ptr(), data_path.as_ptr())
    };
    /* 初始化参数 */

    /* 数据库启动 */
    let ret = unsafe {
        gstor_startup(CString::new(String::from(param.unwrap())).unwrap())
    };
    /* 数据库启动 */

    /* 申请handle */
    let ret = unsafe {
        gstor_alloc(handle.as_ref())
    };
    /* 申请handle */

    /* 打开表 */
    let ret = unsafe {
        gstor_open_table(*handle, CString::new("SYS_KV").unwrap())
    };
    /* 打开表 */


// /* 插入数据 */
//     let ret = unsafe {
//         let key = CString::new("key1").unwrap();
//         let val = CString::new("val1").unwrap();
//         gstor_put(*handle, key.as_ptr(), 4, val.as_ptr(), 4)
//     };
// /* 插入数据 */
//     let key = CString::new("key1").unwrap();
// /* 读取数据 */
//     let ret = unsafe {
//         rust_gstor_get(*handle, key.as_ptr())
//     };
//     let c_str: &CStr = unsafe { CStr::from_ptr(ret) };
//     let str_slice: &str = c_str.to_str().unwrap();
//     let str_buf: String = str_slice.to_owned();
//     println!("[INFO]: key = {:?} value = {}", key, str_buf);
// /* 读取数据 */

    while true {
        let mut rng = rand::thread_rng();
        let random_code = Alphanumeric.sample_string(&mut rng, 32);
        let key: String = "key-".to_string() + random_code.as_ref();
        let val: String = "val-".to_string() + random_code.as_ref();
        // println!("{} {}", key, val);
        /* 插入数据 */
        let ret = unsafe {
            let key = CString::new(key.clone()).unwrap();
            let val = CString::new(val).unwrap();
            gstor_put(*handle, key.as_ptr(), key.as_bytes().len().try_into().unwrap(), val.as_ptr(), val.as_bytes().len().try_into().unwrap())
        };
        /* 插入数据 */

        /* 提交 */
        let ret = unsafe {
            gstor_commit(*handle)
        };
        /* 提交 */

        /* 读取数据 */
        let key = CString::new(key).unwrap();
        let ret = unsafe {
            rust_gstor_get(*handle, key.as_ptr())
        };
        let c_str: &CStr = unsafe { CStr::from_ptr(ret) };
        let str_slice: &str = c_str.to_str().unwrap();
        let str_buf: String = str_slice.to_owned();
        println!("[INFO]: key = {:?} value = {}", key, str_buf);
        /* 读取数据 */

        let ten_millis = time::Duration::from_millis(1000);
        // let now = time::Instant::now();
        thread::sleep(ten_millis);
    }

/* 关闭数据库 */
    unsafe { gstor_shutdown() };
/* 关闭数据库 */
    println!("end...");
}
