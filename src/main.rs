use std::io::Empty;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;

fn main() -> Result<()> {
    leak()?;
    println!("sleep");
    std::thread::sleep(Duration::from_secs(1000));
    Ok(())
}

fn leak() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("col1", DataType::Utf8, true)]));

    let mut writer = ArrowWriter::try_new(Empty::default(), schema.clone(), None)?;
    for _ in 0..1000 {
        let batch = gen_batch(1000, schema.clone());
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

fn gen_batch(size: usize, schema: SchemaRef) -> RecordBatch {
    let mut buf = StringBuilder::new();
    for i in 0..size {
        let s = vec![i as u8; 1000];
        let s = String::from_utf8_lossy(&s);
        buf.append_value(s);
    }
    let col1 = Arc::new(buf.finish());
    RecordBatch::try_new(schema, vec![col1]).unwrap()
}
