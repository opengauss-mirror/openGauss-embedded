extern crate libc;
use std::ffi::CString;
use std::ffi::CStr;
use libc::c_char;
use rand::distributions::{Alphanumeric,DistString};
use std::{thread, time, env, process};
use std::time::{Duration, Instant};


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

    if args.len() < 3 {
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

    let thread_num : u16;
    let total : u16;
    if args.len()==5{
        thread_num = args[3].parse().expect("Wanted a number");
        total = args[4].parse().expect("Wanted a number");
        if thread_num==0||total==0||thread_num>total{
            println!("Invalid parameter args:{:?}", args);
            process::exit(-1);
        }    
    }else{
        thread_num = 8;
        total = 200;
        println!("Invalid parameter args:{:?}", args); 
    }

    let data_path = CString::new(String::from(param.unwrap())).unwrap();
    let handle = Box::<u64>::new(0);
    let mut handles = vec![Box::<u64>::new(0);thread_num.into()];
    
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
    // let ret = unsafe {
    //     gstor_alloc(handle.as_ref())
    // };
    for i in &handles{
        let ret = unsafe {
            gstor_alloc(i.as_ref())
        };
    }
    /* 申请handle */

    /* 打开表 */
    // let ret = unsafe {
    //     gstor_open_table(*handle, CString::new("SYS_KV").unwrap())
    // };
    for i in &handles{
        let ret = unsafe {
            gstor_open_table(**i, CString::new("SYS_KV").unwrap())
        };
    }
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

    // while true {
    //     let mut rng = rand::thread_rng();
    //     let random_code = Alphanumeric.sample_string(&mut rng, 32);
    //     let key: String = "key-".to_string() + random_code.as_ref();
    //     let val: String = "val-".to_string() + random_code.as_ref();
    //     // println!("{} {}", key, val);
    //     /* 插入数据 */
    //     let ret = unsafe {
    //         let key = CString::new(key.clone()).unwrap();
    //         let val = CString::new(val).unwrap();
    //         gstor_put(*handles[0], key.as_ptr(), key.as_bytes().len().try_into().unwrap(), val.as_ptr(), val.as_bytes().len().try_into().unwrap())
    //     };
    //     /* 插入数据 */

    //     /* 提交 */
    //     let ret = unsafe {
    //         gstor_commit(*handles[0])
    //     };
    //     /* 提交 */

    //     /* 读取数据 */
    //     let key = CString::new(key).unwrap();
    //     let ret = unsafe {
    //         rust_gstor_get(*handles[0], key.as_ptr())
    //     };
    //     let c_str: &CStr = unsafe { CStr::from_ptr(ret) };
    //     let str_slice: &str = c_str.to_str().unwrap();
    //     let str_buf: String = str_slice.to_owned();
    //     println!("[INFO]: key = {:?} value = {}", key, str_buf);
    //     /* 读取数据 */

    //     let ten_millis = time::Duration::from_millis(1000);
    //     // let now = time::Instant::now();
    //     thread::sleep(ten_millis);
    // }

    println!("thread_num:{} total:{}",thread_num,total);
    let now = Instant::now();

    let avg_num = total/thread_num;
    let mut thread_handles = vec![];
    for num in 0..thread_num{
        //let index: _ = num.into();
        let index: _ = <u16 as Into<usize>>::into(num);
        let h = *handles[index];
        let thread_handle = thread::spawn(move || do_something(h,num,avg_num));
        thread_handles.push(thread_handle);
    }

    for thread_handle in thread_handles {
        thread_handle.join().unwrap();
    }

/* 关闭数据库 */
    unsafe { gstor_shutdown() };
/* 关闭数据库 */
    println!("end...");
    println!("process done! thread_num:{} total:{} Time spent:{}",thread_num,total,now.elapsed().as_secs());
}

fn do_something(handle: u64, thread_num:u16, total:u16) {
    let now = Instant::now();
    for i in 0..total{
        let mut rng = rand::thread_rng();
        let random_code = Alphanumeric.sample_string(&mut rng, 32);
        let key: String = "key-".to_string() + random_code.as_ref();
        let val: String = "val-".to_string() + random_code.as_ref();
        // println!("{} {}", key, val);
        /* 插入数据 */
        let ret = unsafe {
            let key = CString::new(key.clone()).unwrap();
            let val = CString::new(val).unwrap();
            gstor_put(handle, key.as_ptr(), key.as_bytes().len().try_into().unwrap(), val.as_ptr(), val.as_bytes().len().try_into().unwrap())
        };
        /* 插入数据 */

        /* 提交 */
        let ret = unsafe {
            gstor_commit(handle)
        };
        /* 提交 */

        /* 读取数据 */
        let key = CString::new(key).unwrap();
        let ret = unsafe {
            rust_gstor_get(handle, key.as_ptr())
        };
        let c_str: &CStr = unsafe { CStr::from_ptr(ret) };
        let str_slice: &str = c_str.to_str().unwrap();
        let str_buf: String = str_slice.to_owned();
        if i % 10 == 0{
            println!("[INFO]: thread_num:{}, data number:{} key = {:?} value = {}", thread_num, i, key, str_buf);
        }
        /* 读取数据 */

        //let ten_millis = time::Duration::from_millis(1000);
        // let now = time::Instant::now();
        //thread::sleep(ten_millis);
    }
    println!("thread_num:{} done, deal with number:{}, Time spent:{}",thread_num,total,now.elapsed().as_secs());
}