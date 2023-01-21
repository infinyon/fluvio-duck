mod bind;
mod consume;
mod partition;
mod topic;

mod top {
    use std::ffi::{c_char, c_void};

    use anyhow::Result;
    use libduckdb_sys::{duckdb_database, duckdb_library_version};
    use tracing::info;

    use crate::{
        bind::Database, partition::fluvio_admin_partition_function_def,
        topic::fluvio_admin_topic_function_def,
    };

    use super::consume::fluvio_consumer_table_function_def;

    #[no_mangle]
    pub unsafe extern "C" fn fluvioduck_init_rust(db: *mut c_void) {
        fluvio_future::subscriber::init_tracer(None);
        info!("init");
        init(db).expect("init failed");
        // do nothing
    }

    #[no_mangle]
    pub extern "C" fn fluvioduck_version_rust() -> *const c_char {
        unsafe { duckdb_library_version() }
    }

    fn init(db: duckdb_database) -> Result<()> {
        fluvio_future::subscriber::init_tracer(None);
        let db = Database::from(db);
        let connection = db.connect()?;
        connection.register_table_function(fluvio_consumer_table_function_def())?;
        connection.register_table_function(fluvio_admin_topic_function_def())?;
        connection.register_table_function(fluvio_admin_partition_function_def())?;
        Ok(())
    }
}
