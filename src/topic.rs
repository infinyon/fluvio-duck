use std::boxed::Box;
use std::ffi::c_void;

use anyhow::Result;
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_free, duckdb_function_info, duckdb_init_info,
};
use tracing::debug;

use fluvio::metadata::topic::TopicSpec;

use fluvio::{Fluvio, FluvioAdmin};
use fluvio_future::task::run_block_on;

use crate::bind::{
    malloc_struct, BindInfo, DataChunk, DuckDBTypeEnum, FunctionInfo, InitInfo, LogicalType,
    TableFunction,
};

pub fn fluvio_admin_topic_function_def() -> TableFunction {
    let table_function = TableFunction::new();
    table_function.set_name("fluvio_topic");

    table_function.set_function(Some(topic_read));
    table_function.set_init(Some(topic_init));
    table_function.set_bind(Some(topic_bind));
    table_function
}

struct FluvioAdminInner {
    admin: Box<FluvioAdmin>,
}

#[repr(C)]
struct TopicBindDataStruct(*mut FluvioAdminInner);

#[repr(C)]
struct TopicInitDataStruct {
    done: bool,
}

/// set up duck db table columns
#[no_mangle]
unsafe extern "C" fn topic_bind(bind_ptr: duckdb_bind_info) {
    let bind_info = BindInfo::from(bind_ptr);
    if let Err(err) = internal_bind(&bind_info) {
        bind_info.set_error(&err.to_string());
    }
}

unsafe fn internal_bind(bind_info: &BindInfo) -> Result<()> {
    bind_info.add_result_column("name", LogicalType::new(DuckDBTypeEnum::Varchar));
    bind_info.add_result_column("partitions", LogicalType::new(DuckDBTypeEnum::Integer));

    let admin = run_block_on(async {
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;
        Ok(admin) as Result<FluvioAdmin>
    })?;

    let boxed_admin = Box::new(admin);
    let bind_inner = Box::new(FluvioAdminInner { admin: boxed_admin });
    debug!("amin created");

    let my_bind_data = malloc_struct::<TopicBindDataStruct>();
    (*my_bind_data).0 = Box::into_raw(bind_inner);
    bind_info.set_bind_data(my_bind_data.cast(), Some(drop_my_bind_data_struct));

    Ok(())
}

unsafe extern "C" fn drop_my_bind_data_struct(v: *mut c_void) {
    // let actual = v.cast::<MyBindDataStruct>();
    duckdb_free(v);
}

#[no_mangle]
unsafe extern "C" fn topic_init(info: duckdb_init_info) {
    let info = InitInfo::from(info);

    let mut my_init_data = malloc_struct::<TopicInitDataStruct>();
    (*my_init_data).done = false;
    info.set_init_data(my_init_data.cast(), Some(duckdb_free));
}

#[no_mangle]
unsafe extern "C" fn topic_read(info: duckdb_function_info, chunk: duckdb_data_chunk) {
    let info = FunctionInfo::from(info);
    let output = DataChunk::from(chunk);
    if let Err(err) = internal_read(&info, &output) {
        info.set_error(&err.to_string());
    }
}

/// read data from fluvio
#[no_mangle]
unsafe fn internal_read(info: &FunctionInfo, output: &DataChunk) -> Result<()> {
    let _bind_data = info.get_bind_data::<TopicBindDataStruct>();

    let bind_data = info.get_bind_data::<TopicBindDataStruct>();
    let inner = &mut *(*bind_data).0;
    let mut init_data = info.get_init_data::<TopicInitDataStruct>();
    if (*init_data).done {
        output.set_size(0);
        return Ok(());
    }

    let admin = inner.admin.as_ref();
    let topics = run_block_on(async { admin.all::<TopicSpec>().await })?;

    let mut row = 0;
    for topic in topics {
        // name
        let value_vector = output.get_vector(0);
        value_vector.assign_string_element(row, topic.name.as_bytes());

        let offset_vector = output.get_vector(1);
        offset_vector.set_data(row as usize, topic.spec.partitions());
        row += 1;
    }

    output.set_size(row as u64);
    (*init_data).done = true;

    Ok(())
}
