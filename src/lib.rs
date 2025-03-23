extern crate dbase;
extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use dbase::{BufReadWriteFile, FieldIndex, FieldInfo, FieldType, FieldValue, File as DbaseFile};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::sync::{Arc, Mutex};
use std::{
    error::Error,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

// read only once created
#[repr(C)]
struct DbaseBindData {
    filepath: String,
    fields: Vec<FieldInfo>,
    total_rows: usize,
}

#[repr(C)]
struct DbaseInitData {
    current_row: AtomicUsize,
    done: AtomicBool,
    file_handle: Arc<Mutex<DbaseFile<BufReadWriteFile>>>,
    projection: Vec<usize>,
}

struct DbaseVTab;

impl VTab for DbaseVTab {
    type InitData = DbaseInitData;
    type BindData = DbaseBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let filepath = bind.get_parameter(0).to_string();
        let file = DbaseFile::open_read_only(&filepath).expect(
            format!(
                "Could not find file {:?} or corresponding memo file",
                &filepath
            )
            .as_str(),
        );

        let fields = file.fields();

        for field in file.fields() {
            let logical_type = match &field.field_type() {
                FieldType::Character => LogicalTypeId::Varchar,
                FieldType::Currency => LogicalTypeId::Float,
                FieldType::Date => LogicalTypeId::Date,
                FieldType::DateTime => LogicalTypeId::Timestamp,
                FieldType::Double => LogicalTypeId::Double,
                FieldType::Float => LogicalTypeId::Float,
                FieldType::Integer => LogicalTypeId::Integer,
                FieldType::Logical => LogicalTypeId::Boolean,
                FieldType::Memo => LogicalTypeId::Varchar,
                FieldType::Numeric => LogicalTypeId::Double,
            };
            bind.add_result_column(
                &field.name().to_lowercase(),
                LogicalTypeHandle::from(logical_type),
            );
        }

        let total_rows = file.num_records();

        Ok(DbaseBindData {
            filepath,
            fields: fields.to_vec(),
            total_rows,
        })
    }

    fn init(info: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = info.get_bind_data::<DbaseBindData>();
        let filepath = unsafe { &(*bind_data).filepath };
        let projection = info
            .get_column_indices()
            .into_iter()
            .map(|x| x as usize)
            .collect();

        let file = DbaseFile::open_read_only(&filepath)?;

        Ok(DbaseInitData {
            current_row: AtomicUsize::new(0),
            done: AtomicBool::new(false),
            file_handle: Arc::new(Mutex::new(file)),
            projection,
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        let mut dbase_file = init_data
            .file_handle
            .lock()
            .expect("Unable to lock file handle");

        let start_row = init_data
            .current_row
            .load(std::sync::atomic::Ordering::Relaxed);
        if start_row >= bind_data.total_rows || init_data.done.load(Ordering::Relaxed) {
            output.set_len(0);
            init_data.done.store(true, Ordering::Relaxed);
            return Ok(());
        }

        let batch_size = 5.min(bind_data.total_rows - start_row);

        let mut output_row = 0;

        for input_row in 0..batch_size {
            if let Some(mut record) = dbase_file.record(input_row + start_row) {
                if record.is_deleted()? {
                    continue;
                }
                // there is no projection (e.g. select count(1))
                if init_data.projection == [usize::MAX] {
                    output_row += 1;
                    continue;
                }
                for (col_idx, proj) in init_data.projection.iter().enumerate() {
                    let mut vector = output.flat_vector(col_idx);
                    let r = record
                        .read_field(FieldIndex(*proj))
                        .expect("unable to read field");

                    match r {
                        FieldValue::Character(Some(c)) => vector.insert(output_row, c.as_str()),
                        FieldValue::Currency(c) => {
                            let slice = vector.as_mut_slice::<f64>();
                            slice[output_row] = c;
                        }
                        FieldValue::Date(Some(d)) => {
                            let slice = vector.as_mut_slice::<i32>();
                            slice[output_row] = d.to_unix_days();
                        }
                        FieldValue::Double(d) => {
                            let slice = vector.as_mut_slice::<f64>();
                            slice[output_row] = d
                        }
                        FieldValue::Numeric(Some(d)) => {
                            let slice = vector.as_mut_slice::<f64>();
                            slice[output_row] = d
                        }
                        FieldValue::DateTime(dt) => {
                            let slice = vector.as_mut_slice::<i64>();
                            slice[output_row] = dt.to_unix_timestamp();
                        }
                        FieldValue::Float(Some(f)) => {
                            let slice = vector.as_mut_slice::<f32>();
                            slice[output_row] = f;
                        }
                        FieldValue::Integer(i) => {
                            let slice = vector.as_mut_slice::<i32>();
                            slice[output_row] = i;
                        }
                        FieldValue::Logical(Some(b)) => {
                            let slice = vector.as_mut_slice::<bool>();
                            slice[output_row] = b;
                        }
                        FieldValue::Memo(m) => vector.insert(output_row, m.as_str()),
                        FieldValue::Character(None)
                        | FieldValue::Numeric(None)
                        | FieldValue::Logical(None)
                        | FieldValue::Date(None)
                        | FieldValue::Float(None) => vector.set_null(output_row),
                    }
                }
                output_row += 1;
            };
        }
        init_data
            .current_row
            .fetch_add(batch_size, Ordering::Relaxed);
        output.set_len(batch_size);

        Ok(())
    }
    fn supports_pushdown() -> bool {
        true
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

const EXTENSION_NAME: &str = env!("CARGO_PKG_NAME");

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<DbaseVTab>(EXTENSION_NAME)
        .expect("Failed to register hello table function");
    Ok(())
}
