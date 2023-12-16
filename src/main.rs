use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_array::builder::StringBuilder;
use arrow_array::{Array, RecordBatch};
use tokio::io::AsyncWrite;
use parquet::arrow::AsyncArrowWriter;
use random_string::charsets::ALPHANUMERIC;

#[tokio::main]
async fn main() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Utf8, true),
        Field::new("col2", DataType::Utf8, true),
    ]));

    for i in 0..4 {
        println!("round{i}");
        let mut writer = AsyncArrowWriter::try_new(DummyWriter, schema.clone(), 0, None)?;
        // write 1000 batch
        for _ in 0..1_000 {
            let batch = gen_batch(1000, schema.clone());
            writer.write(&batch).await?
        }
        writer.close().await?;
    }

    println!("sleep");
    tokio::time::sleep(Duration::from_secs(1000)).await;
    Ok(())
}

fn gen_batch(size: usize, schema: SchemaRef) -> RecordBatch {
    let a1 = gen_array(size);
    let a2 = gen_array(size);
    RecordBatch::try_new(
        schema,
        vec![a1, a2],
    ).unwrap()
}

fn gen_array(size: usize) -> Arc<dyn Array> {
    let mut buf = StringBuilder::new();
    for _ in 0..size {
        let s = random_string::generate(2000, ALPHANUMERIC);
        buf.append_value(s);
    }
    Arc::new(buf.finish())
}

struct DummyWriter;

impl AsyncWrite for DummyWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
}
