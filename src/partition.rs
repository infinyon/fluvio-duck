use std::boxed::Box;
use std::ffi::c_void;

use anyhow::Result;
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_free, duckdb_function_info, duckdb_init_info,
};
use tracing::debug;

use fluvio::dataplane::record::PartitionError;
use fluvio::metadata::partition::{PartitionSpec, ReplicaKey};

use fluvio::{Fluvio, FluvioAdmin};
use fluvio_future::task::run_block_on;

use crate::bind::{
    malloc_struct, BindInfo, DataChunk, DuckDBTypeEnum, FunctionInfo, InitInfo, LogicalType,
    TableFunction,
};

pub fn fluvio_admin_partition_function_def() -> TableFunction {
    let table_function = TableFunction::new();
    table_function.set_name("fluvio_partition");

    table_function.set_function(Some(partition_read));
    table_function.set_init(Some(partition_init));
    table_function.set_bind(Some(partition_bind));
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

#[no_mangle]
unsafe extern "C" fn partition_bind(bind_ptr: duckdb_bind_info) {
    let bind_info = BindInfo::from(bind_ptr);
    if let Err(err) = internal_bind(&bind_info) {
        bind_info.set_error(&err.to_string());
    }
}

unsafe fn internal_bind(bind_info: &BindInfo) -> Result<()> {
    bind_info.add_result_column("topic", LogicalType::new(DuckDBTypeEnum::Varchar));
    bind_info.add_result_column("partition", LogicalType::new(DuckDBTypeEnum::Varchar));
    bind_info.add_result_column("LEO", LogicalType::new(DuckDBTypeEnum::Integer));
    //  bind_info.add_result_column("SIZE", LogicalType::new(DuckDBTypeEnum::Integer));

    let admin = run_block_on(async {
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;
        Ok(admin) as Result<FluvioAdmin>
    })?;

    let boxed_admin = Box::new(admin);
    debug!("amin created");
    let bind_inner = Box::new(FluvioAdminInner { admin: boxed_admin });

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
unsafe extern "C" fn partition_init(info: duckdb_init_info) {
    let info = InitInfo::from(info);

    let mut my_init_data = malloc_struct::<TopicInitDataStruct>();
    (*my_init_data).done = false;
    info.set_init_data(my_init_data.cast(), Some(duckdb_free));
}

#[no_mangle]
unsafe extern "C" fn partition_read(info: duckdb_function_info, chunk: duckdb_data_chunk) {
    let info = FunctionInfo::from(info);
    let output = DataChunk::from(chunk);
    if let Err(err) = internal_read(&info, &output) {
        info.set_error(&err.to_string());
    }
}

unsafe fn internal_read(info: &FunctionInfo, output: &DataChunk) -> Result<()> {
    let bind_data = info.get_bind_data::<TopicBindDataStruct>();
    let inner = &mut *(*bind_data).0;
    let mut init_data = info.get_init_data::<TopicInitDataStruct>();
    if (*init_data).done {
        output.set_size(0);
        return Ok(());
    }

    let admin = inner.admin.as_ref();
    let partitions = run_block_on(async { admin.all::<PartitionSpec>().await })?;

    let mut row = 0;
    for partition in partitions {
        // name

        let (topic, partition_key) = {
            let parse_key: Result<ReplicaKey, PartitionError> = partition.name.clone().try_into();
            match parse_key {
                Ok(key) => {
                    let (topic, partition) = key.split();
                    (topic, partition.to_string())
                }
                Err(err) => (err.to_string(), "-1".to_owned()),
            }
        };

        let topic_vector = output.get_vector(0);
        topic_vector.assign_string_element(row, topic.as_bytes());

        let partition_vector = output.get_vector(1);
        partition_vector.assign_string_element(row, partition_key.as_bytes());

        let leo_vector = output.get_vector(2);
        leo_vector.set_data(row as usize, partition.status.leader.leo as u32);

        row += 1;
    }

    output.set_size(row as u64);
    (*init_data).done = true;

    Ok(())
}
