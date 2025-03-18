use std::ffi::{CStr, CString};
use std::os::raw::c_char;

// #[repr(C)]
// pub struct User {
//     age: u8,
//     name: String
// }

// #[no_mangle]
// pub extern fn struct_to_c() -> User {
//     // let user = User { name: CString::new("catalina").unwrap(), age: 25 };
//     let user = User { name: String::from("struct in rust"), age: 255 };
//     user
// }

// #[no_mangle]
// pub extern fn say_hello(name: *const std::os::raw::c_char) {
//     let c_str: &CStr = unsafe {
//         CStr::from_ptr(name)
//     };
//     println!("{}", "hello, ".to_string() + c_str.to_str().unwrap());
// }

#[no_mangle]
pub extern fn rust_alloc_init(block_initial_size: usize, page_cnt_per_block: usize, page_size: usize) -> i32 {
    let mut alloc = ALLOC_8K.lock().unwrap();

    if alloc.blocks.len() > 0 {
        return 1;
    }
        // alloc.page_cnt_per_block = 200;
    alloc.init(block_initial_size, page_cnt_per_block, page_size);
    0
}

#[no_mangle]
pub extern fn rust_get_page(index: usize) -> *const Page {
    let mut alloc = ALLOC_8K.lock().unwrap();
    
    let mut page = match alloc.get_page(index) {
        Ok(page) => page,
        Err(msg) => {
            println!("{}", msg);
            null_mut()
        }
    };
    println!("0: {:?}", page);

    page 
}


use std::ptr::null_mut;
use std::vec::Vec;
use lazy_static::lazy_static;
use std::{sync::Mutex};

pub struct Page {
    buf: Vec<u8>,
}

impl Page {
    fn new() -> Self {
        Page {
            buf: Vec::new(),
        }
    }

    fn init(&mut self, length: usize) {
        self.buf.resize(length, 0u8);
    }
}
struct Block {
    elements:   Vec<Page>,
    used_count: u64,
}

impl Block {
    fn new() -> Self {
        Block { elements: Vec::new(), used_count: 0 }
    }

    fn init(&mut self, page_cnt: usize, page_size: usize) {
        self.elements.reserve(page_cnt);
        for _ in 0..page_cnt {
            let mut p = Page::new();
            p.init(page_size);
            self.elements.push(p);
        }
    }
}


pub struct MyAllocator {
    blocks: Vec<Block>,
    page_cnt_per_block: usize,
}

impl MyAllocator {
    pub fn new() -> Self {
        MyAllocator{
            blocks: Vec::new(),
            page_cnt_per_block: 0,
        }
    }
    pub fn init(&mut self, block_initial_size: usize, page_cnt_per_block: usize, page_size: usize) {
        self.blocks.reserve(block_initial_size);
        println!("self.blocks = {}", self.blocks.len());

        self.page_cnt_per_block = page_cnt_per_block;
        for i in 0.. block_initial_size {
            let mut b =  Block::new();
            b.init(page_cnt_per_block, page_size);
            self.blocks.push(b);
            self.blocks[i].used_count = 0;
        }
        println!("self.blocks = {}", self.blocks.len());
    }
}

// impl FromStr for Endian {
//     type Err = String;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         match s {
//             "little" => Ok(Self::Little),
//             "big" => Ok(Self::Big),
//             _ => Err(format!(r#"unknown endian: "{}""#, s)),
//         }
//     }
// }

type AllocatorErr = String;

pub trait MemoryAllocator {
    // add code here
    // fn malloc() -> (AllocatorErr, *const Page, usize){

    // }

    fn free(_idx: usize) -> Option<AllocatorErr> {
        None
    }

    fn get_page(&self, idx: usize) -> Result<*const Page, AllocatorErr>;
    
    fn reszie_buffer(_new_size: usize) -> Option<AllocatorErr>;
}

impl MemoryAllocator for MyAllocator {
    fn get_page(&self, idx: usize) -> Result<*const Page, AllocatorErr> {
        let block_cap = self.blocks.capacity();
        if idx >= self.page_cnt_per_block * block_cap{
            Err(AllocatorErr::from("The index is not in range!"))
        } else {
            let mut block_index = idx / self.page_cnt_per_block;  
            let mut page_index = idx % self.page_cnt_per_block;
            if page_index == 0 {
                page_index = self.page_cnt_per_block - 1;
                block_index = block_index - 1;
            } else {
                page_index = page_index - 1;
            }
            Ok(&self.blocks[block_index].elements[page_index])
        }
    }

    fn reszie_buffer(_new_size: usize) -> Option<AllocatorErr> {
        None
    }
}

lazy_static! {
    static ref ALLOC_8K: Mutex<MyAllocator> = {
        let a = MyAllocator::new();
        Mutex::new(a)
    };
}