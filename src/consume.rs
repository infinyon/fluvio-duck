use std::boxed::Box;
use std::ffi::c_void;
use std::pin::Pin;

use anyhow::{anyhow, Result};
use chrono::DateTime;
use futures_lite::{Stream, StreamExt};
use tracing::{debug, error, trace};

use jql::walker;
use libduckdb_sys::{
    duckdb_bind_info, duckdb_data_chunk, duckdb_free, duckdb_function_info, duckdb_init_info,
    duckdb_vector_size, idx_t,
};

use fluvio::consumer::Record;
use fluvio::dataplane::link::ErrorCode;
use fluvio::{ConsumerConfig, PartitionConsumer};
use fluvio_future::task::run_block_on;
use serde_json::Value;

use crate::bind::{
    malloc_struct, BindInfo, DataChunk, DuckDBTypeEnum, FunctionInfo, InitInfo, LogicalType,
    TableFunction,
};

pub fn fluvio_consumer_table_function_def() -> TableFunction {
    let table_function = TableFunction::new();
    table_function.set_name("fluvio_consume");

    // first parameter is topic name
    table_function.add_parameter(&LogicalType::new(DuckDBTypeEnum::Varchar));
    // second paramter is offset
    //   table_function.add_parameter(&LogicalType::new(DuckDBTypeEnum::Bigint));
    //third paramter is count
    //   table_function.add_parameter(&LogicalType::new(DuckDBTypeEnum::Bigint));

    table_function.set_function(Some(consumer_read));
    table_function.set_init(Some(consume_init));
    table_function.set_bind(Some(consumer_bind));
    //  table_function.set_projection_pushdown(true);
    table_function
}

type PartitionConsumerIteratorInner = Pin<Box<dyn Stream<Item = Result<Record, ErrorCode>> + Send>>;

struct FluvioBindInner {
    consumer_stream: PartitionConsumerIteratorInner,
    _consumer: Box<PartitionConsumer>,
    max_row_count: u64,
    columns: Vec<ColumnMapping>,
}

#[repr(C)]
struct FluvioBindDataStruct(*mut FluvioBindInner);

#[repr(C)]
struct FluvioInitDataStruct {
    total_row: u64,
}

/// read data from fluvio
#[no_mangle]
unsafe extern "C" fn consumer_read(info: duckdb_function_info, chunk: duckdb_data_chunk) {
    let info = FunctionInfo::from(info);
    let output = DataChunk::from(chunk);
    if let Err(err) = internal_read(&info, &output) {
        info.set_error(&err.to_string());
    }
}

unsafe fn internal_read(info: &FunctionInfo, output: &DataChunk) -> Result<()> {
    let bind_data = info.get_bind_data::<FluvioBindDataStruct>();
    let inner = &mut *(*bind_data).0;
    let max_row_count = inner.max_row_count;
    let mut init_data = info.get_init_data::<FluvioInitDataStruct>();

    // current accumulated total
    let mut accum_total = (*init_data).total_row;
    if accum_total >= max_row_count {
        debug!(accum_total, "done, max row reached");
        output.set_size(0);
        return Ok(());
    }

    let max_len = duckdb_vector_size() as usize;
    let mut row: usize = 0;
    loop {
        debug!("row: {}", row);
        // if exceed max row or max duckdb len
        if accum_total >= max_row_count || row >= max_len {
            output.set_size(row as u64);
            (*init_data).total_row = accum_total;
            break;
        } else {
            let mut stream = inner.consumer_stream.as_mut();
            debug!("waiting for data from fluvio");
            // get first data from fluvio
            let record_output = run_block_on(async { stream.next().await });

            let record = match record_output {
                Some(record) => record?,
                None => {
                    debug!("no more records");
                    output.set_size(row as u64);
                    (*init_data).total_row = max_row_count; // set to max so we can terminate
                    break;
                }
            };

            let offset = record.offset();
            debug!(offset, "retrieved record offset");
            trace!(
                "offset: {offset}, value: {:?}",
                std::str::from_utf8(record.value()).unwrap()
            );

            for (column_index, column) in inner.columns.iter().enumerate() {
                if let Err(err) =
                    column
                        .mapping
                        .map(&record, column_index as u64, row, &output, &column.ty)
                {
                    info.set_error(&err.to_string());
                }
            }

            row += 1;
            accum_total += 1;
        }
    }

    Ok(())
}

#[no_mangle]
unsafe extern "C" fn consume_init(info: duckdb_init_info) {
    let info = InitInfo::from(info);

    let _column_count = info.column_count();
    //println!("columt count: {}", column_count);

    let _colum_1 = info.projected_column_index(0);
    //println!("projected column: {}", colum_1);

    let mut my_init_data = malloc_struct::<FluvioInitDataStruct>();
    (*my_init_data).total_row = 0;
    info.set_init_data(my_init_data.cast(), Some(duckdb_free));
    debug!("consumer init done");
}

const PARM_TOPIC_NAME: u64 = 0;
//const PARM_START_OFFSET: u64 = 1;
//const PARM_FETCH_COUNT: u64 = 2;

/// set up duck db table columns
#[no_mangle]
unsafe extern "C" fn consumer_bind(bind_ptr: duckdb_bind_info) {
    let bind_info = BindInfo::from(bind_ptr);
    if let Err(err) = internal_bind(&bind_info) {
        bind_info.set_error(&err.to_string());
    }
}

unsafe fn internal_bind(bind_info: &BindInfo) -> Result<()> {
    let param_count = bind_info.get_parameter_count();
    // we need at least one parameter
    assert_eq!(param_count, PARM_TOPIC_NAME + 1);

    let topic_param = bind_info.get_parameter(PARM_TOPIC_NAME);
    let ptr = topic_param.get_varchar();
    let cmd_args = ptr.to_str()?;

    let consumer_opt = opt::ConsumeOpt::parse_from_string(cmd_args)?;
    let config = consumer_opt.generate_config()?;
    let start_offset = consumer_opt.calculate_offset()?;

    let topic = consumer_opt.topic.clone();
    let consumer = run_block_on(async { fluvio::consumer(topic, 0).await })?;
    let boxed_consumer = Box::new(consumer);
    debug!("consumer created");

    // add offset column which is integer
    let columns = consumer_opt.columns_mappings();
    for column in columns.iter() {
        bind_info.add_result_column(&column.name, LogicalType::new(column.ty.clone()));
    }

    let consumer_stream = run_block_on(async {
        boxed_consumer
            .stream_with_config(start_offset, config)
            .await
    })?;

    let boxed_stream = consumer_stream.boxed();
    // println!("consumer stream created");
    let bind_inner = Box::new(FluvioBindInner {
        consumer_stream: boxed_stream,
        _consumer: boxed_consumer,
        max_row_count: consumer_opt.rows as u64,
        columns,
    });

    let my_bind_data = malloc_struct::<FluvioBindDataStruct>();
    (*my_bind_data).0 = Box::into_raw(bind_inner);
    bind_info.set_bind_data(my_bind_data.cast(), Some(drop_my_bind_data_struct));
    debug!("bind done");

    Ok(())
}

unsafe extern "C" fn drop_my_bind_data_struct(v: *mut c_void) {
    // let actual = v.cast::<MyBindDataStruct>();
    duckdb_free(v);
}

pub(crate) struct ColumnMapping {
    pub(crate) name: String,
    pub(crate) mapping: Box<dyn MappingTrait>,
    pub(crate) ty: DuckDBTypeEnum,
}

impl ColumnMapping {
    /// create new mapping for column,
    pub(crate) fn new(name_ty: String, mapping: Box<dyn MappingTrait>) -> Self {
        // check if name contains type
        let mut parts = name_ty.split(':');
        let name = parts.next().unwrap().to_string(); // always name
        let ty = if let Some(ty_string) = parts.next() {
            match ty_string {
                "i" => DuckDBTypeEnum::Integer,
                "l" => DuckDBTypeEnum::Uinteger,
                "f" => DuckDBTypeEnum::Float,
                "d" => DuckDBTypeEnum::Double,
                "s" => DuckDBTypeEnum::Varchar,
                "t" => DuckDBTypeEnum::TimestampMs,
                _ => DuckDBTypeEnum::Varchar,
            }
        } else {
            DuckDBTypeEnum::Varchar
        };

        Self { name, mapping, ty }
    }
}

/// map record to string
pub(crate) trait MappingTrait {
    // map record to chunk for column
    fn map(
        &self,
        record: &Record,
        colum: idx_t,
        row: usize,
        output: &DataChunk,
        ty: &DuckDBTypeEnum,
    ) -> Result<()>;
}

struct OffsetMapper();

impl MappingTrait for OffsetMapper {
    fn map(
        &self,
        record: &Record,
        colum: idx_t,
        row: usize,
        output: &DataChunk,
        _ty: &DuckDBTypeEnum,
    ) -> Result<()> {
        let offset_vector = output.get_vector(colum);
        offset_vector.set_data(row, record.offset() as u32);
        Ok(())
    }
}

struct TimestampMapper();

impl MappingTrait for TimestampMapper {
    fn map(
        &self,
        record: &Record,
        colum: idx_t,
        row: usize,
        output: &DataChunk,
        _ty: &DuckDBTypeEnum,
    ) -> Result<()> {
        let timestamp_vector = output.get_vector(colum);
        timestamp_vector.set_data(row, record.timestamp() as u64);
        Ok(())
    }
}

struct ValueMapper();

impl MappingTrait for ValueMapper {
    fn map(
        &self,
        record: &Record,
        colum: idx_t,
        row: usize,
        output: &DataChunk,
        _ty: &DuckDBTypeEnum,
    ) -> Result<()> {
        let value_vector = output.get_vector(colum);
        value_vector.assign_string_element(row as idx_t, record.value());
        Ok(())
    }
}

struct JqlMapper(String);

impl JqlMapper {
    pub(crate) fn new(jql: String) -> Self {
        Self(jql)
    }
}

impl MappingTrait for JqlMapper {
    fn map(
        &self,
        record: &Record,
        colum: idx_t,
        row: usize,
        output: &DataChunk,
        ty: &DuckDBTypeEnum,
    ) -> Result<()> {
        let v: Value = serde_json::from_slice(record.value())?;
        let find_value = match walker(&v, &self.0) {
            Ok(value) => value,
            Err(err) => {
                let value_vector = output.get_vector(colum);
                value_vector.assign_string_element(row as idx_t, err.to_string().as_bytes());
                return Ok(());
            }
        };

        match find_value {
            Value::String(s) => {
                debug!("string: {}", s);
                match ty {
                    DuckDBTypeEnum::TimestampMs => {
                        // 2023-01-28T23:54:23.405Z
                        let value_vector = output.get_vector(colum);
                        // parse timestamp
                        match DateTime::parse_from_rfc3339(&s) {
                            Ok(dt) => {
                                let timestamp = dt.timestamp_millis();
                                value_vector.set_data(row, timestamp as u64);
                            }
                            Err(err) => {
                                error!("error parsing timestamp: {}", err);
                            }
                        }
                    }
                    _ => {
                        let value_vector = output.get_vector(colum);
                        value_vector.assign_string_element(row as idx_t, s.as_bytes());
                    }
                }
            }
            Value::Number(n) => {
                debug!("number: {}", n);
                match ty {
                    DuckDBTypeEnum::Integer => {
                        if let Some(i) = n.as_i64() {
                            let val = i as i32;
                            let value_vector = output.get_vector(colum);
                            value_vector.set_data(row, val);
                        }
                    }
                    DuckDBTypeEnum::Uinteger => {
                        if let Some(i) = n.as_i64() {
                            let val = i as u64;
                            let value_vector = output.get_vector(colum);
                            value_vector.set_data(row, val);
                        }
                    }
                    DuckDBTypeEnum::Float => {
                        if let Some(f) = n.as_f64() {
                            let val = f as f32;
                            let value_vector = output.get_vector(colum);
                            value_vector.set_data(row, val);
                        }
                    }
                    DuckDBTypeEnum::Double => {
                        if let Some(f) = n.as_f64() {
                            let val = f as f64;
                            let value_vector = output.get_vector(colum);
                            value_vector.set_data(row, val);
                        }
                    }
                    _ => {
                        debug!("ignore number: {}", n);
                    }
                }
            }
            Value::Bool(b) => {
                debug!("bool: {}", b);
                let value_vector = output.get_vector(colum);
                value_vector.set_data(row, b as u8);
            }
            Value::Null => {
                debug!("null value");
                match ty {
                    DuckDBTypeEnum::Integer => {
                        let value_vector = output.get_vector(colum);
                        value_vector.set_data(row, 0 as i32);
                    }
                    DuckDBTypeEnum::Uinteger => {
                        let value_vector = output.get_vector(colum);
                        value_vector.set_data(row, 0 as i64);
                    }
                    DuckDBTypeEnum::Float => {
                        let value_vector = output.get_vector(colum);
                        value_vector.set_data(row, 0 as f64);
                    }
                    DuckDBTypeEnum::Double => {
                        let value_vector = output.get_vector(colum);
                        value_vector.set_data(row, 0 as f64);
                    }
                    DuckDBTypeEnum::Varchar => {
                        let value_vector = output.get_vector(colum);
                        let string = "null";
                        value_vector.assign_string_element(row as idx_t, string.as_bytes());
                    }
                    _ => {}
                }
            }
            Value::Object(_) => {
                debug!("object: {:?}", find_value);
                let value_vector = output.get_vector(colum);
                value_vector.assign_string_element(row as idx_t, find_value.to_string().as_bytes());
            }
            _ => {
                debug!("other: {:?}", find_value);
                let value_vector = output.get_vector(colum);
                value_vector.assign_string_element(row as idx_t, find_value.to_string().as_bytes());
            }
        }

        Ok(())
    }
}

mod opt {

    use std::{collections::BTreeMap, path::PathBuf};

    use clap::Parser;

    use fluvio::{
        FluvioError, Isolation, Offset, SmartModuleContextData, SmartModuleInvocation,
        SmartModuleInvocationWasm, SmartModuleKind,
    };
    use fluvio_future::tracing::debug;
    use fluvio_smartengine::transformation::TransformationConfig;
    use fluvio_types::PartitionId;

    use super::*;

    /// copy from Fluvio CLI
    ///
    /// By default, consume operates in "streaming" mode, where the command will remain
    /// active and wait for new messages, printing them as they arrive. You can use the
    /// '-d' flag to exit after consuming all available messages.
    #[derive(Debug, Parser)]
    pub struct ConsumeOpt {
        /// Topic name
        #[clap(value_name = "topic")]
        pub topic: String,

        /// Partition id
        #[clap(short = 'p', long, default_value = "0", value_name = "integer")]
        pub partition: PartitionId,

        /// Consume records from all partitions
        #[clap(short = 'A', long = "all-partitions", conflicts_with_all = &["partition"])]
        pub all_partitions: bool,

        /// Disable continuous processing of messages
        #[clap(short = 'd', long)]
        pub enable_continuous: bool,

        /// Consume records from the beginning of the log
        #[clap(short = 'B', long,  conflicts_with_all = &["head","start", "tail"])]
        pub beginning: bool,

        /// Consume records starting <integer> from the beginning of the log
        #[clap(short = 'H', long, value_name = "integer", conflicts_with_all = &["beginning", "start", "tail"])]
        pub head: Option<u32>,

        /// Consume records starting <integer> from the end of the log
        #[clap(short = 'T', long,  value_name = "integer", conflicts_with_all = &["beginning","head", "start"])]
        pub tail: Option<u32>,

        /// The absolute offset of the first record to begin consuming from
        #[clap(long, value_name = "integer", conflicts_with_all = &["beginning", "head", "tail"])]
        pub start: Option<u32>,

        #[clap(long, default_value = "1000")]
        pub rows: u32,

        /// Consume records until end offset (inclusive)
        #[clap(long, value_name = "integer")]
        pub end: Option<u32>,

        /// Maximum number of bytes to be retrieved
        #[clap(short = 'b', long = "maxbytes", value_name = "integer")]
        pub max_bytes: Option<i32>,

        /// (Optional) Path to a file to use as an initial accumulator value with --aggregate
        #[clap(long, requires = "aggregate_group", alias = "a-init")]
        pub aggregate_initial: Option<String>,

        /// (Optional) Extra input parameters passed to the smartmodule module.
        /// They should be passed using key=value format
        /// Eg. fluvio consume topic-name --filter filter.wasm -e foo=bar -e key=value -e one=1
        #[clap(
            short = 'e',
            requires = "smartmodule_group",
            long="params",
            value_parser=parse_key_val,
            // value_parser,
            // action,
            number_of_values = 1
        )]
        pub params: Option<Vec<(String, String)>>,

        /// Isolation level that consumer must respect.
        /// Supported values: read_committed (ReadCommitted) - consume only committed records,
        /// read_uncommitted (ReadUncommitted) - consume all records accepted by leader.
        #[clap(long, value_parser=parse_isolation)]
        pub isolation: Option<Isolation>,

        /// Name of the smartmodule
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm"
        )]
        pub smartmodule: Option<String>,

        /// Path to the smart module
        #[clap(
            long,
            group("smartmodule_group"),
            group("aggregate_group"),
            alias = "sm_path"
        )]
        pub smartmodule_path: Option<PathBuf>,

        /// (Optional) Path to a file with transformation specification.
        #[clap(long, conflicts_with = "smartmodule_group")]
        pub transforms_file: Option<PathBuf>,

        /// (Optional) Transformation specification as JSON formatted string.
        /// E.g. fluvio consume topic-name --transform='{"uses":"infinyon/jolt@0.1.0","with":{"spec":"[{\"operation\":\"default\",\"spec\":{\"source\":\"test\"}}]"}}'
        #[clap(long, short, conflicts_with_all = &["smartmodule_group", "transforms_file"])]
        pub transform: Vec<String>,

        /// column mapping, this will map to duckdb columns, if this not specific, then default column (key,timestamp, value)
        /// this assume values json format
        /// Eg. -c  ph=contact.ph -c addr=contact.addr -e
        #[clap(
            short = 'c',
            long,
            value_parser=parse_key_val,
        )]
        pub columns: Vec<(String, String)>,
    }

    impl ConsumeOpt {
        pub fn parse_from_string(input: &str) -> Result<Self> {
            let wrapper = format!("ConsumeOpt {}", input);
            let args = wrapper.split_whitespace();
            ConsumeOpt::try_parse_from(args).map_err(|err| err.into())
        }

        pub fn calculate_offset(&self) -> Result<Offset> {
            if let Some(end_offset) = self.end {
                if let Some(start_offset) = self.start {
                    if end_offset < start_offset {
                        eprintln!(
                            "Argument end-offset must be greater than or equal to specified start offset"
                        );
                        return Err(FluvioError::CrossingOffsets(start_offset, end_offset).into());
                    }
                }
            }

            let offset = if self.beginning {
                Offset::from_beginning(0)
            } else if let Some(offset) = self.head {
                Offset::from_beginning(offset)
            } else if let Some(offset) = self.start {
                Offset::absolute(offset as i64).unwrap()
            } else if let Some(offset) = self.tail {
                Offset::from_end(offset)
            } else {
                Offset::end()
            };

            Ok(offset)
        }

        pub(crate) fn columns_mappings(&self) -> Vec<ColumnMapping> {
            let columns: Vec<ColumnMapping> = self
                .columns
                .iter()
                .map(|(name, json_map)| {
                    ColumnMapping::new(name.clone(), Box::new(JqlMapper::new(json_map.clone())))
                })
                .collect();

            if columns.is_empty() {
                vec![
                    ColumnMapping {
                        name: "offset".to_owned(),
                        mapping: Box::new(OffsetMapper()),
                        ty: DuckDBTypeEnum::Integer,
                    },
                    ColumnMapping {
                        name: "timestamp".to_owned(),
                        mapping: Box::new(TimestampMapper()),
                        ty: DuckDBTypeEnum::TimestampMs,
                    },
                    ColumnMapping {
                        name: "value".to_string(),
                        mapping: Box::new(ValueMapper()),
                        ty: DuckDBTypeEnum::Varchar,
                    },
                ]
            } else {
                columns
            }
        }

        pub fn generate_config(&self) -> Result<ConsumerConfig> {
            let mut builder = ConsumerConfig::builder();
            if let Some(max_bytes) = self.max_bytes {
                builder.max_bytes(max_bytes);
            }

            let initial_param = match &self.params {
                None => BTreeMap::default(),
                Some(params) => params.clone().into_iter().collect(),
            };

            let smart_module = if let Some(smart_module_name) = &self.smartmodule {
                vec![create_smartmodule(
                    smart_module_name,
                    self.smart_module_ctx(),
                    initial_param,
                )]
            } else if !self.transform.is_empty() {
                let config =
                    TransformationConfig::try_from(self.transform.clone()).map_err(|err| {
                        anyhow!(format!("unable to parse `transform` argument: {}", err))
                    })?;
                create_smartmodule_list(config)?
            } else if let Some(transforms_file) = &self.transforms_file {
                let config = TransformationConfig::from_file(transforms_file).map_err(|err| {
                    anyhow!(format!(
                        "unable to process `transforms_file` argument: {}",
                        err
                    ))
                })?;
                create_smartmodule_list(config)?
            } else {
                Vec::new()
            };

            builder.smartmodule(smart_module);

            builder.disable_continuous(!self.enable_continuous);

            if let Some(isolation) = self.isolation {
                builder.isolation(isolation);
            }

            let consume_config = builder.build()?;
            debug!("consume config: {:#?}", consume_config);

            Ok(consume_config)
        }

        fn smart_module_ctx(&self) -> SmartModuleContextData {
            if let Some(agg_initial) = &self.aggregate_initial {
                SmartModuleContextData::Aggregate {
                    accumulator: agg_initial.clone().into_bytes(),
                }
            } else {
                SmartModuleContextData::None
            }
        }
    }

    fn parse_key_val(s: &str) -> Result<(String, String)> {
        let pos = s
            .find('=')
            .ok_or_else(|| anyhow!(format!("invalid KEY=value: no `=` found in `{}`", s)))?;
        Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
    }

    pub(crate) fn parse_isolation(s: &str) -> Result<Isolation, String> {
        match s {
            "read_committed" | "ReadCommitted" | "readCommitted" | "readcommitted" => Ok(Isolation::ReadCommitted),
            "read_uncommitted" | "ReadUncommitted" | "readUncommitted" | "readuncommitted" => Ok(Isolation::ReadUncommitted),
            _ => Err(format!("unrecognized isolation: {}. Supported: read_committed (ReadCommitted), read_uncommitted (ReadUncommitted)", s)),
        }
    }

    fn create_smartmodule(
        name: &str,
        ctx: SmartModuleContextData,
        params: BTreeMap<String, String>,
    ) -> SmartModuleInvocation {
        SmartModuleInvocation {
            wasm: SmartModuleInvocationWasm::Predefined(name.to_string()),
            kind: SmartModuleKind::Generic(ctx),
            params: params.into(),
        }
    }

    /// create list of smartmodules from a list of transformations
    fn create_smartmodule_list(config: TransformationConfig) -> Result<Vec<SmartModuleInvocation>> {
        Ok(config
            .transforms
            .into_iter()
            .map(|t| SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::Predefined(t.uses),
                kind: SmartModuleKind::Generic(Default::default()),
                params: t
                    .with
                    .into_iter()
                    .map(|(k, v)| (k, v.into()))
                    .collect::<std::collections::BTreeMap<String, String>>()
                    .into(),
            })
            .collect())
    }
}
