
pub trait Create {
    fn get_table_colums_info(&self) -> Vec<codegen::utils::model::StColumnDef4C>;
    fn get_table_indexs(&self) -> Vec<codegen::utils::model::StIndexDef>;
    fn get_table_colums_info2(&self) -> Vec<interface::data::exp_column_def_t>;
    fn create_table(&self, conn: &interface::Interface, handle: u64) ->std::result::Result<i32, codegen::utils::error::Error> ;
    fn create_index(&self, conn: &interface::Interface, handle: u64) ->std::result::Result<i32 ,codegen::utils::error::Error> ;
}