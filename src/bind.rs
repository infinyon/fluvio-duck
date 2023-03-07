use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::DerefMut;
use std::os::raw::c_char;
use std::ptr::null_mut;
use std::{ffi::CString, ops::Deref};

use anyhow::{anyhow, Result};
use libduckdb_sys::{
    duckdb_bind_add_result_column, duckdb_bind_get_parameter, duckdb_bind_get_parameter_count,
    duckdb_bind_info, duckdb_bind_set_bind_data, duckdb_bind_set_error, duckdb_connect,
    duckdb_connection, duckdb_create_logical_type, duckdb_create_table_function, duckdb_data_chunk,
    duckdb_data_chunk_get_vector, duckdb_data_chunk_set_size, duckdb_database,
    duckdb_delete_callback_t, duckdb_destroy_table_function, duckdb_destroy_value,
    duckdb_function_get_bind_data, duckdb_function_get_init_data, duckdb_function_info,
    duckdb_function_set_error, duckdb_get_int64, duckdb_get_varchar, duckdb_init_get_column_count,
    duckdb_init_get_column_index, duckdb_init_info, duckdb_init_set_init_data, duckdb_logical_type,
    duckdb_malloc, duckdb_register_table_function, duckdb_table_function,
    duckdb_table_function_add_parameter, duckdb_table_function_bind_t,
    duckdb_table_function_init_t, duckdb_table_function_set_bind,
    duckdb_table_function_set_extra_info, duckdb_table_function_set_function,
    duckdb_table_function_set_init, duckdb_table_function_set_name,
    duckdb_table_function_supports_projection_pushdown, duckdb_table_function_t, duckdb_value,
    duckdb_vector, duckdb_vector_assign_string_element_len, duckdb_vector_get_data,
    duckdb_vector_size, idx_t, DuckDBSuccess, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    DUCKDB_TYPE_DUCKDB_TYPE_BLOB, DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN, DUCKDB_TYPE_DUCKDB_TYPE_DATE,
    DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE, DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
    DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    DUCKDB_TYPE_DUCKDB_TYPE_INTEGER, DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    DUCKDB_TYPE_DUCKDB_TYPE_LIST, DUCKDB_TYPE_DUCKDB_TYPE_MAP, DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
    DUCKDB_TYPE_DUCKDB_TYPE_STRUCT, DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_UNION,
    DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UUID, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
};

#[macro_export]
macro_rules! as_string {
    ($x:expr) => {
        std::ffi::CString::new($x)
            .expect("c string")
            .as_ptr()
            .cast::<c_char>()
    };
}

pub unsafe fn malloc_struct<T>() -> *mut T {
    duckdb_malloc(size_of::<T>()).cast::<T>()
}

///
#[derive(Debug)]
pub struct BindInfo(pub(crate) duckdb_bind_info);

impl BindInfo {
    pub fn get_parameter(&self, param_index: u64) -> Value {
        unsafe { Value::from(duckdb_bind_get_parameter(self.0, param_index)) }
    }

    pub fn add_result_column(&self, column_name: &str, ty: LogicalType) {
        unsafe {
            duckdb_bind_add_result_column(self.0, as_string!(column_name), *ty);
        }
    }

    pub fn set_bind_data(&self, data: *mut c_void, free_function: duckdb_delete_callback_t) {
        unsafe { duckdb_bind_set_bind_data(self.0, data, free_function) }
    }

    pub fn get_parameter_count(&self) -> u64 {
        unsafe { duckdb_bind_get_parameter_count(self.0) }
    }

    pub fn set_error(&self, error: &str) {
        unsafe {
            duckdb_bind_set_error(self.0, as_string!(error));
        }
    }
}

impl From<duckdb_bind_info> for BindInfo {
    fn from(duck_bind: duckdb_bind_info) -> Self {
        Self(duck_bind)
    }
}

pub struct Value(pub(crate) duckdb_value);

impl Value {
    pub fn get_varchar(&self) -> CString {
        unsafe { CString::from_raw(duckdb_get_varchar(self.0)) }
    }

    #[allow(unused)]
    pub fn get_int64(&self) -> i64 {
        unsafe { duckdb_get_int64(self.0) }
    }
}

impl From<duckdb_value> for Value {
    fn from(value: duckdb_value) -> Self {
        Self(value)
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_value(&mut self.0);
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
#[repr(u32)]
pub enum DuckDBTypeEnum {
    Boolean = DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    Tinyint = DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
    Smallint = DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
    /// Signed 32-bit integer
    Integer = DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    Bigint = DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    Utinyint = DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    Usmallint = DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    Uinteger = DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
    Ubigint = DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    Float = DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
    Double = DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    Timestamp = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
    Date = DUCKDB_TYPE_DUCKDB_TYPE_DATE,
    Time = DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    Interval = DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    Hugeint = DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    Varchar = DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    Blob = DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
    Decimal = DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    TimestampS = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    TimestampMs = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    TimestampNs = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
    Enum = DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
    List = DUCKDB_TYPE_DUCKDB_TYPE_LIST,
    Struct = DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
    Map = DUCKDB_TYPE_DUCKDB_TYPE_MAP,
    Uuid = DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    Union = DUCKDB_TYPE_DUCKDB_TYPE_UNION,
}

pub struct LogicalType(pub(crate) duckdb_logical_type);

impl Default for LogicalType {
    fn default() -> Self {
        Self::new(DuckDBTypeEnum::Varchar)
    }
}

impl LogicalType {
    pub fn new(typ: DuckDBTypeEnum) -> Self {
        unsafe {
            Self(duckdb_create_logical_type(
                typ as libduckdb_sys::duckdb_type,
            ))
        }
    }
}

impl Deref for LogicalType {
    type Target = duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct TableFunction(duckdb_table_function);

impl TableFunction {
    pub fn new() -> Self {
        Self(unsafe { duckdb_create_table_function() })
    }

    pub fn set_name(&self, name: &str) -> &TableFunction {
        unsafe {
            let string = CString::from_vec_unchecked(name.as_bytes().into());
            duckdb_table_function_set_name(self.0, string.as_ptr());
        }
        self
    }

    pub fn add_parameter(&self, logical_type: &LogicalType) -> &Self {
        unsafe {
            duckdb_table_function_add_parameter(self.0, **logical_type);
        }
        self
    }

    pub fn set_function(&self, func: duckdb_table_function_t) -> &Self {
        unsafe {
            duckdb_table_function_set_function(self.0, func);
        }
        self
    }

    pub fn set_init(&self, init_func: duckdb_table_function_init_t) -> &Self {
        unsafe {
            duckdb_table_function_set_init(self.0, init_func);
        }
        self
    }

    pub fn set_bind(&self, bind_func: duckdb_table_function_bind_t) -> &Self {
        unsafe {
            duckdb_table_function_set_bind(self.0, bind_func);
        }
        self
    }

    #[allow(dead_code)]
    pub fn set_extra_info(&self, extra_info: *mut c_void, destroy: duckdb_delete_callback_t) {
        unsafe { duckdb_table_function_set_extra_info(self.0, extra_info, destroy) };
    }

    /// set projection
    #[allow(unused)]
    pub fn set_projection_pushdown(&self, flag: bool) {
        unsafe { duckdb_table_function_supports_projection_pushdown(self.0, flag) };
    }
}

impl Deref for TableFunction {
    type Target = duckdb_table_function;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TableFunction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for TableFunction {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_table_function(&mut self.0);
        }
    }
}

pub struct Database(duckdb_database);

impl From<duckdb_database> for Database {
    fn from(db: duckdb_database) -> Self {
        Self(db)
    }
}

impl Database {
    pub fn connect(&self) -> Result<Connection> {
        let mut connection: duckdb_connection = null_mut();

        unsafe {
            if duckdb_connect(self.0, &mut connection) != DuckDBSuccess {
                return Err(anyhow!("Failed to connect to database"));
            }
        }

        Ok(Connection::from(connection))
    }
}

pub struct Connection(duckdb_connection);

impl From<duckdb_connection> for Connection {
    fn from(connection: duckdb_connection) -> Self {
        Self(connection)
    }
}

impl Connection {
    pub fn register_table_function(&self, table_function: TableFunction) -> Result<()> {
        unsafe {
            if duckdb_register_table_function(self.0, *table_function) != DuckDBSuccess {
                return Err(anyhow!("Failed to register table function"));
            }
        }
        Ok(())
    }
}

pub struct FunctionInfo(duckdb_function_info);

impl From<duckdb_function_info> for FunctionInfo {
    fn from(info: duckdb_function_info) -> Self {
        Self(info)
    }
}

impl FunctionInfo {
    pub fn get_bind_data<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_bind_data(self.0).cast() }
    }

    pub fn get_init_data<T>(&self) -> *mut T {
        unsafe { duckdb_function_get_init_data(self.0).cast() }
    }

    pub fn set_error(&self, error: &str) {
        unsafe {
            duckdb_function_set_error(self.0, as_string!(error));
        }
    }
}

pub struct InitInfo(duckdb_init_info);
impl From<duckdb_init_info> for InitInfo {
    fn from(info: duckdb_init_info) -> Self {
        Self(info)
    }
}

impl InitInfo {
    pub fn set_init_data(&self, data: *mut c_void, freeer: duckdb_delete_callback_t) {
        unsafe { duckdb_init_set_init_data(self.0, data, freeer) };
    }

    pub fn column_count(&self) -> idx_t {
        unsafe { duckdb_init_get_column_count(self.0) }
    }

    pub fn projected_column_index(&self, column_index: idx_t) -> idx_t {
        unsafe { duckdb_init_get_column_index(self.0, column_index) }
    }
}

pub struct DataChunk(duckdb_data_chunk);

impl From<duckdb_data_chunk> for DataChunk {
    fn from(chunk: duckdb_data_chunk) -> Self {
        Self(chunk)
    }
}

impl DataChunk {
    pub fn get_vector<T>(&self, column_index: idx_t) -> Vector<T> {
        Vector::from(unsafe { duckdb_data_chunk_get_vector(self.0, column_index) })
    }

    pub fn set_size(&self, size: idx_t) {
        unsafe { duckdb_data_chunk_set_size(self.0, size) };
    }
}

pub struct Vector<T> {
    duck_ptr: duckdb_vector,
    _phantom: PhantomData<T>,
}

impl<T> From<duckdb_vector> for Vector<T> {
    fn from(duck_ptr: duckdb_vector) -> Self {
        Self {
            duck_ptr,
            _phantom: PhantomData,
        }
    }
}

impl<T> Vector<T> {
    /// set data
    pub fn set_data(&self, row: usize, data: T) {
        let data_ptr: *mut T = unsafe { duckdb_vector_get_data(self.duck_ptr).cast() };
        let data_slice: &mut [T] =
            unsafe { std::slice::from_raw_parts_mut(data_ptr, duckdb_vector_size() as usize) };
        data_slice[row] = data;
    }
}

impl Vector<&[u8]> {
    pub fn assign_string_element(&self, index: idx_t, str: &[u8]) {
        unsafe {
            duckdb_vector_assign_string_element_len(
                self.duck_ptr,
                index,
                str.as_ptr() as *const c_char,
                str.len() as idx_t,
            )
        }
    }
}
