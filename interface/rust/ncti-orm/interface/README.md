# interface README
引用代码
interface文件夹是rust的调用存储引擎适配层C接口的lib库，如果要引用该库，需要以下几个步骤：
    1. 将整个文件夹代码加入到工程根目录下
    2. 修改根目录下Cargo.toml文件，将interface添加到members中，如果没有members，则添加members，例如：
        members = [
            ".",
            "interface",
        ]
    3. 修改interface下的build.rs脚本中的rustc-link-search选项，将最后面的路径替换成gstor动态库的目录，例如：
        println!("cargo:rustc-link-search=native=/opengauss-embedded/3rd/gstor/output/lib");
    4. 调用引用，在main.rs中引入interface模块，使用如下语句：
        use interface;
        然后在引入模块类和函数即可：
        use crate::interface::{Interface, GS_FALSE, GS_TRUE };

调用代码（暂时）
    1.	先使用Interface的new函数新建interface对象：
        let mut conn = Interface::new(data_path, DB_TYPE_GSTOR);
    2.	调用打开数据库接口：
        conn.open_db();
    3.	初始化数据库handle：
        conn.init_handle();
    4.	即可调用操作函数，open、create、update等函数，例如：
        conn.create_table(table_name.clone(), column_list.len() as libc::c_int, column_list.as_ptr(), 0 as libc::c_int, index_list.as_ptr());
