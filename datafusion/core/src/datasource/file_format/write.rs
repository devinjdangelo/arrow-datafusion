// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Module containing helper methods/traits related to enabling
//! write support for the various file formats

use std::collections::HashMap;
use std::io::Error;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::listing::{ListingTableUrl, PartitionedFile};
use crate::datasource::physical_plan::{FileMeta, FileSinkConfig};
use crate::error::Result;
use crate::physical_plan::SendableRecordBatchStream;

use arrow_array::builder::UInt64Builder;
use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, StructArray};
use arrow_schema::{DataType, Schema};
use datafusion_common::cast::as_string_array;
use datafusion_common::{exec_err, DataFusionError};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_execution::TaskContext;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::{ready, StreamExt};
use object_store::path::Path;
use object_store::{MultipartId, ObjectMeta, ObjectStore};
use rand::distributions::DistString;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::{JoinHandle, JoinSet};
use tokio::try_join;

/// `AsyncPutWriter` is an object that facilitates asynchronous writing to object stores.
/// It is specifically designed for the `object_store` crate's `put` method and sends
/// whole bytes at once when the buffer is flushed.
pub struct AsyncPutWriter {
    /// Object metadata
    object_meta: ObjectMeta,
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    /// A buffer that stores the bytes to be sent
    current_buffer: Vec<u8>,
    /// Used for async handling in flush method
    inner_state: AsyncPutState,
}

impl AsyncPutWriter {
    /// Constructor for the `AsyncPutWriter` object
    pub fn new(object_meta: ObjectMeta, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_meta,
            store,
            current_buffer: vec![],
            // The writer starts out in buffering mode
            inner_state: AsyncPutState::Buffer,
        }
    }

    /// Separate implementation function that unpins the [`AsyncPutWriter`] so
    /// that partial borrows work correctly
    fn poll_shutdown_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        loop {
            match &mut self.inner_state {
                AsyncPutState::Buffer => {
                    // Convert the current buffer to bytes and take ownership of it
                    let bytes = Bytes::from(mem::take(&mut self.current_buffer));
                    // Set the inner state to Put variant with the bytes
                    self.inner_state = AsyncPutState::Put { bytes }
                }
                AsyncPutState::Put { bytes } => {
                    // Send the bytes to the object store's put method
                    return Poll::Ready(
                        ready!(self
                            .store
                            .put(&self.object_meta.location, bytes.clone())
                            .poll_unpin(cx))
                        .map_err(Error::from),
                    );
                }
            }
        }
    }
}

/// An enum that represents the inner state of AsyncPut
enum AsyncPutState {
    /// Building Bytes struct in this state
    Buffer,
    /// Data in the buffer is being sent to the object store
    Put { bytes: Bytes },
}

impl AsyncWrite for AsyncPutWriter {
    // Define the implementation of the AsyncWrite trait for the `AsyncPutWriter` struct
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        // Extend the current buffer with the incoming buffer
        self.current_buffer.extend_from_slice(buf);
        // Return a ready poll with the length of the incoming buffer
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Return a ready poll with an empty result
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        // Call the poll_shutdown_inner method to handle the actual sending of data to the object store
        self.poll_shutdown_inner(cx)
    }
}

/// Stores data needed during abortion of MultiPart writers
pub(crate) struct MultiPart {
    /// A shared reference to the object store
    store: Arc<dyn ObjectStore>,
    multipart_id: MultipartId,
    location: Path,
}

impl MultiPart {
    /// Create a new `MultiPart`
    pub fn new(
        store: Arc<dyn ObjectStore>,
        multipart_id: MultipartId,
        location: Path,
    ) -> Self {
        Self {
            store,
            multipart_id,
            location,
        }
    }
}

pub(crate) enum AbortMode {
    Put,
    Append,
    MultiPart(MultiPart),
}

/// A wrapper struct with abort method and writer
pub(crate) struct AbortableWrite<W: AsyncWrite + Unpin + Send> {
    writer: W,
    mode: AbortMode,
}

impl<W: AsyncWrite + Unpin + Send> AbortableWrite<W> {
    /// Create a new `AbortableWrite` instance with the given writer, and write mode.
    pub(crate) fn new(writer: W, mode: AbortMode) -> Self {
        Self { writer, mode }
    }

    /// handling of abort for different write modes
    pub(crate) fn abort_writer(&self) -> Result<BoxFuture<'static, Result<()>>> {
        match &self.mode {
            AbortMode::Put => Ok(async { Ok(()) }.boxed()),
            AbortMode::Append => exec_err!("Cannot abort in append mode"),
            AbortMode::MultiPart(MultiPart {
                store,
                multipart_id,
                location,
            }) => {
                let location = location.clone();
                let multipart_id = multipart_id.clone();
                let store = store.clone();
                Ok(Box::pin(async move {
                    store
                        .abort_multipart(&location, &multipart_id)
                        .await
                        .map_err(DataFusionError::ObjectStore)
                }))
            }
        }
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncWrite for AbortableWrite<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, Error>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Error>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}

/// An enum that defines different file writer modes.
#[derive(Debug, Clone, Copy)]
pub enum FileWriterMode {
    /// Data is appended to an existing file.
    Append,
    /// Data is written to a new file.
    Put,
    /// Data is written to a new file in multiple parts.
    PutMultipart,
}
/// A trait that defines the methods required for a RecordBatch serializer.
#[async_trait]
pub trait BatchSerializer: Unpin + Send {
    /// Asynchronously serializes a `RecordBatch` and returns the serialized bytes.
    async fn serialize(&mut self, batch: RecordBatch) -> Result<Bytes>;
    /// Duplicates self to support serializing multiple batches in parallel on multiple cores
    fn duplicate(&mut self) -> Result<Box<dyn BatchSerializer>> {
        Err(DataFusionError::NotImplemented(
            "Parallel serialization is not implemented for this file type".into(),
        ))
    }
}

/// Returns an [`AbortableWrite`] which writes to the given object store location
/// with the specified compression
pub(crate) async fn create_writer(
    writer_mode: FileWriterMode,
    file_compression_type: FileCompressionType,
    file_meta: FileMeta,
    object_store: Arc<dyn ObjectStore>,
) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
    let object = &file_meta.object_meta;
    match writer_mode {
        // If the mode is append, call the store's append method and return wrapped in
        // a boxed trait object.
        FileWriterMode::Append => {
            let writer = object_store
                .append(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Append,
            );
            Ok(writer)
        }
        // If the mode is put, create a new AsyncPut writer and return it wrapped in
        // a boxed trait object
        FileWriterMode::Put => {
            let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
            let writer = AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::Put,
            );
            Ok(writer)
        }
        // If the mode is put multipart, call the store's put_multipart method and
        // return the writer wrapped in a boxed trait object.
        FileWriterMode::PutMultipart => {
            let (multipart_id, writer) = object_store
                .put_multipart(&object.location)
                .await
                .map_err(DataFusionError::ObjectStore)?;
            Ok(AbortableWrite::new(
                file_compression_type.convert_async_writer(writer)?,
                AbortMode::MultiPart(MultiPart::new(
                    object_store,
                    multipart_id,
                    object.location.clone(),
                )),
            ))
        }
    }
}

type WriterType = AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>;
type SerializerType = Box<dyn BatchSerializer>;

/// Serializes a single data stream in parallel and writes to an ObjectStore
/// concurrently. Data order is preserved. In the event of an error,
/// the ObjectStore writer is returned to the caller in addition to an error,
/// so that the caller may handle aborting failed writes.
pub(crate) async fn serialize_rb_stream_to_object_store(
    mut data_rx: Receiver<RecordBatch>,
    mut serializer: Box<dyn BatchSerializer>,
    mut writer: AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>,
    unbounded_input: bool,
) -> std::result::Result<(WriterType, u64), (WriterType, DataFusionError)> {
    let (tx, mut rx) =
        mpsc::channel::<JoinHandle<Result<(usize, Bytes), DataFusionError>>>(100);

    let serialize_task = tokio::spawn(async move {
        while let Some(batch) = data_rx.recv().await {
            match serializer.duplicate() {
                Ok(mut serializer_clone) => {
                    let handle = tokio::spawn(async move {
                        let num_rows = batch.num_rows();
                        let bytes = serializer_clone.serialize(batch).await?;
                        Ok((num_rows, bytes))
                    });
                    tx.send(handle).await.map_err(|_| {
                        DataFusionError::Internal(
                            "Unknown error writing to object store".into(),
                        )
                    })?;
                    if unbounded_input {
                        tokio::task::yield_now().await;
                    }
                }
                Err(_) => {
                    return Err(DataFusionError::Internal(
                        "Unknown error writing to object store".into(),
                    ))
                }
            }
        }
        Ok(())
    });

    let mut row_count = 0;
    while let Some(handle) = rx.recv().await {
        match handle.await {
            Ok(Ok((cnt, bytes))) => {
                match writer.write_all(&bytes).await {
                    Ok(_) => (),
                    Err(e) => {
                        return Err((
                            writer,
                            DataFusionError::Execution(format!(
                                "Error writing to object store: {e}"
                            )),
                        ))
                    }
                };
                row_count += cnt;
            }
            Ok(Err(e)) => {
                // Return the writer along with the error
                return Err((writer, e));
            }
            Err(e) => {
                // Handle task panic or cancellation
                return Err((
                    writer,
                    DataFusionError::Execution(format!(
                        "Serialization task panicked or was cancelled: {e}"
                    )),
                ));
            }
        }
    }

    match serialize_task.await {
        Ok(Ok(_)) => (),
        Ok(Err(e)) => return Err((writer, e)),
        Err(_) => {
            return Err((
                writer,
                DataFusionError::Internal("Unknown error writing to object store".into()),
            ))
        }
    };
    Ok((writer, row_count as u64))
}

type RecordBatchReceiver = Receiver<RecordBatch>;
type DemuxedStreamReceiver = Receiver<(Path, RecordBatchReceiver)>;

/// Dynamically partitions input stream to acheive desired maximum rows per file
async fn row_count_demuxer(
    mut tx: Sender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    single_file_output: bool,
) -> Result<()> {
    let exec_options = &context.session_config().options().execution;
    let max_rows_per_file = exec_options.soft_max_rows_per_output_file;
    let max_buffered_batches = exec_options.max_buffered_batches_per_output_file;
    let mut total_rows_current_file = 0;
    let mut part_idx = 0;
    let write_id =
        rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let mut tx_file = create_new_file_stream(
        &base_output_path,
        &write_id,
        part_idx,
        &file_extension,
        single_file_output,
        max_buffered_batches,
        &mut tx,
    )
    .await?;
    part_idx += 1;

    while let Some(rb) = input.next().await.transpose()? {
        total_rows_current_file += rb.num_rows();
        tx_file.send(rb).await.map_err(|_| {
            DataFusionError::Execution("Error sending RecordBatch to file stream!".into())
        })?;

        if total_rows_current_file >= max_rows_per_file && !single_file_output {
            total_rows_current_file = 0;
            tx_file = create_new_file_stream(
                &base_output_path,
                &write_id,
                part_idx,
                &file_extension,
                single_file_output,
                max_buffered_batches,
                &mut tx,
            )
            .await?;
            part_idx += 1;
        }
    }
    Ok(())
}

/// Splits an input stream based on the distinct values of a set of columns
/// Assumes standard hive style partition paths such as
/// /col1=val1/col2=val2/outputfile.parquet
async fn hive_style_partitions_demuxer(
    tx: Sender<(Path, Receiver<RecordBatch>)>,
    mut input: SendableRecordBatchStream,
    context: Arc<TaskContext>,
    partition_by: Vec<(String, DataType)>,
    base_output_path: ListingTableUrl,
    file_extension: String,
) -> Result<()> {
    let write_id =
        rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

    let exec_options = &context.session_config().options().execution;
    let max_buffered_recordbatches = exec_options.max_buffered_batches_per_output_file;

    // To support non string partition col types, cast the type to &str first
    let mut value_map: HashMap<Vec<String>, Sender<RecordBatch>> = HashMap::new();

    while let Some(rb) = input.next().await.transpose()? {
        let mut all_partition_values = vec![];

        // First compute partition key for each row of batch, e.g. (col1=val1, col2=val2, ...)
        for (col, dtype) in partition_by.iter() {
            let mut partition_values = vec![];
            let col_array =
                rb.column_by_name(col)
                    .ok_or(DataFusionError::Execution(format!(
                        "PartitionBy Column {} does not exist in source data!",
                        col
                    )))?;

            match dtype{
                DataType::Utf8 => {
                    let array = as_string_array(col_array)?;
                    for i in 0..rb.num_rows(){
                        partition_values.push(array.value(i));
                    }
                },
                _ => return Err(DataFusionError::NotImplemented(format!("it is not yet supported to write to hive partitions with datatype {}", dtype)))
            }

            all_partition_values.push(partition_values);
        }

        // Next compute how the batch should be split up to take each distinct key to its own batch
        let mut take_map = HashMap::new();
        for i in 0..rb.num_rows() {
            let mut part_key = vec![];
            for vals in all_partition_values.iter() {
                part_key.push(vals[i].to_owned());
            }
            let builder = take_map.entry(part_key).or_insert(UInt64Builder::new());
            builder.append_value(i as u64);
        }

        // Divide up the batch into distinct partition key batches and send each batch
        for (part_key, mut builder) in take_map.into_iter() {
            // Take method adapted from https://github.com/lancedb/lance/pull/1337/files
            // TODO: upstream RecordBatch::take to arrow-rs
            let take_indices = builder.finish();
            let struct_array: StructArray = rb.clone().into();
            let parted_batch = RecordBatch::try_from(
                arrow::compute::take(&struct_array, &take_indices, None)?.as_struct(),
            )
            .map_err(|_| {
                DataFusionError::Internal("Unexpected error partitioning batch!".into())
            })?;

            let part_tx = match value_map.get_mut(&part_key) {
                Some(part_tx) => part_tx,
                None => {
                    // Create channel for previously unseen distinct partition key and notify consumer of new file
                    let (part_tx, part_rx) = tokio::sync::mpsc::channel::<RecordBatch>(
                        max_buffered_recordbatches,
                    );
                    let mut file_path = base_output_path.prefix().clone();
                    for j in 0..part_key.len() {
                        file_path = file_path
                            .child(format!("{}={}", partition_by[j].0, part_key[j]));
                    }
                    file_path =
                        file_path.child(format!("{}.{}", write_id, file_extension));

                    tx.send((file_path, part_rx)).await.map_err(|_| {
                        DataFusionError::Execution(
                            "Error sending new file stream!".into(),
                        )
                    })?;

                    value_map.insert(part_key.clone(), part_tx);
                    value_map
                        .get_mut(&part_key)
                        .ok_or(DataFusionError::Internal(
                            "Key must exist since it was just inserted!".into(),
                        ))?
                }
            };

            // remove partitions columns
            let end_idx = parted_batch.num_columns() - partition_by.len();
            let non_part_cols = &parted_batch.columns()[..end_idx];
            let mut non_part_fields = vec![];
            'outer: for field in parted_batch.schema().all_fields() {
                let name = field.name();
                for (part_name, _) in partition_by.iter() {
                    if name == part_name {
                        continue 'outer;
                    }
                }
                non_part_fields.push(field.to_owned())
            }
            let schema = Schema::new(non_part_fields);
            let final_batch_to_send =
                RecordBatch::try_new(Arc::new(schema), non_part_cols.into())?;

            // Finally send the partial batch partitioned by distinct value!
            part_tx.send(final_batch_to_send).await.map_err(|_| {
                DataFusionError::Internal("Unexpected error sending parted batch!".into())
            })?;
        }
    }

    Ok(())
}

/// Splits a single [SendableRecordBatchStream] into a dynamically determined
/// number of partitions at execution time. The partitions are determined by
/// factors known only at execution time, such as total number of rows and
/// partition column values. The demuxer task communicates to the caller
/// by sending channels over a channel. The inner channels send RecordBatches
/// which should be contained within the same output file. The outer channel
/// is used to send a dynamic number of inner channels, representing a dynamic
/// number of total output files. The caller is also responsible to monitor
/// the demux task for errors and abort accordingly. The single_file_ouput parameter
/// overrides all other settings to force only a single file to be written.
/// partition_by parameter will additionally split the input based on the unique
/// values of a specific column `<https://github.com/apache/arrow-datafusion/issues/7744>``
///                                                                              ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     ┌──────▶ │  batch 1  ├────▶...──────▶│   Batch a  │    │ Output File1│
///                                                                     │        └───────────┘               └────────────┘    └─────────────┘
///                                                                     │
///                                                 ┌──────────┐        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
/// ┌───────────┐               ┌────────────┐      │          │        ├──────▶ │  batch a+1├────▶...──────▶│   Batch b  │    │ Output File2│
/// │  batch 1  ├────▶...──────▶│   Batch N  ├─────▶│  Demux   ├────────┤ ...    └───────────┘               └────────────┘    └─────────────┘
/// └───────────┘               └────────────┘      │          │        │
///                                                 └──────────┘        │        ┌───────────┐               ┌────────────┐    ┌─────────────┐
///                                                                     └──────▶ │  batch d  ├────▶...──────▶│   Batch n  │    │ Output FileN│
///                                                                              └───────────┘               └────────────┘    └─────────────┘
pub(crate) fn start_demuxer_task(
    input: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    partition_by: Option<Vec<(String, DataType)>>,
    base_output_path: ListingTableUrl,
    file_extension: String,
    single_file_output: bool,
) -> (JoinHandle<Result<()>>, DemuxedStreamReceiver) {
    let exec_options = &context.session_config().options().execution;
    let max_parallel_files = exec_options.max_parallel_ouput_files;

    let (tx, rx) = tokio::sync::mpsc::channel(max_parallel_files);
    let context = context.clone();
    let task: JoinHandle<std::result::Result<(), DataFusionError>> = match partition_by {
        Some(parts) => tokio::spawn(async move {
            hive_style_partitions_demuxer(
                tx,
                input,
                context,
                parts,
                base_output_path,
                file_extension,
            )
            .await
        }),
        None => tokio::spawn(async move {
            row_count_demuxer(
                tx,
                input,
                context,
                base_output_path,
                file_extension,
                single_file_output,
            )
            .await
        }),
    };

    (task, rx)
}

fn generate_file_path(
    base_output_path: &ListingTableUrl,
    write_id: &str,
    part_idx: usize,
    file_extension: &str,
    single_file_output: bool,
) -> Path {
    if !single_file_output {
        base_output_path
            .prefix()
            .child(format!("{}_{}.{}", write_id, part_idx, file_extension))
    } else {
        base_output_path.prefix().to_owned()
    }
}

async fn create_new_file_stream(
    base_output_path: &ListingTableUrl,
    write_id: &str,
    part_idx: usize,
    file_extension: &str,
    single_file_output: bool,
    max_buffered_batches: usize,
    tx: &mut Sender<(Path, Receiver<RecordBatch>)>,
) -> Result<Sender<RecordBatch>> {
    let file_path = generate_file_path(
        base_output_path,
        write_id,
        part_idx,
        file_extension,
        single_file_output,
    );
    let (tx_file, rx_file) = mpsc::channel(max_buffered_batches / 2);
    tx.send((file_path, rx_file)).await.map_err(|_| {
        DataFusionError::Execution("Error sending RecordBatch to file stream!".into())
    })?;
    Ok(tx_file)
}

type FileWriteBundle = (Receiver<RecordBatch>, SerializerType, WriterType);
/// Contains the common logic for serializing RecordBatches and
/// writing the resulting bytes to an ObjectStore.
/// Serialization is assumed to be stateless, i.e.
/// each RecordBatch can be serialized without any
/// dependency on the RecordBatches before or after.
pub(crate) async fn stateless_serialize_and_write_files(
    mut rx: Receiver<FileWriteBundle>,
    tx: tokio::sync::oneshot::Sender<u64>,
    unbounded_input: bool,
) -> Result<()> {
    let mut row_count = 0;
    // tracks if any writers encountered an error triggering the need to abort
    let mut any_errors = false;
    // tracks the specific error triggering abort
    let mut triggering_error = None;
    // tracks if any errors were encountered in the process of aborting writers.
    // if true, we may not have a guarentee that all written data was cleaned up.
    let mut any_abort_errors = false;
    let mut join_set = JoinSet::new();
    while let Some((data_rx, serializer, writer)) = rx.recv().await {
        join_set.spawn(async move {
            serialize_rb_stream_to_object_store(
                data_rx,
                serializer,
                writer,
                unbounded_input,
            )
            .await
        });
    }
    let mut finished_writers = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => match res {
                Ok((writer, cnt)) => {
                    finished_writers.push(writer);
                    row_count += cnt;
                }
                Err((writer, e)) => {
                    finished_writers.push(writer);
                    any_errors = true;
                    triggering_error = Some(e);
                }
            },
            Err(e) => {
                // Don't panic, instead try to clean up as many writers as possible.
                // If we hit this code, ownership of a writer was not joined back to
                // this thread, so we cannot clean it up (hence any_abort_errors is true)
                any_errors = true;
                any_abort_errors = true;
                triggering_error = Some(DataFusionError::Internal(format!(
                    "Unexpected join error while serializing file {e}"
                )));
            }
        }
    }

    // Finalize or abort writers as appropriate
    for mut writer in finished_writers.into_iter() {
        match any_errors {
            true => {
                let abort_result = writer.abort_writer();
                if abort_result.is_err() {
                    any_abort_errors = true;
                }
            }
            false => {
                writer.shutdown()
                    .await
                    .map_err(|_| DataFusionError::Internal("Error encountered while finalizing writes! Partial results may have been written to ObjectStore!".into()))?;
            }
        }
    }

    if any_errors {
        match any_abort_errors{
            true => return Err(DataFusionError::Internal("Error encountered during writing to ObjectStore and failed to abort all writers. Partial result may have been written.".into())),
            false => match triggering_error {
                Some(e) => return Err(e),
                None => return Err(DataFusionError::Internal("Unknown Error encountered during writing to ObjectStore. All writers succesfully aborted.".into()))
            }
        }
    }

    tx.send(row_count).map_err(|_| {
        DataFusionError::Internal(
            "Error encountered while sending row count back to file sink!".into(),
        )
    })?;
    Ok(())
}

/// Orchestrates multipart put of a dynamic number of output files from a single input stream
/// for any statelessly serialized file type. That is, any file type for which each [RecordBatch]
/// can be serialized independently of all other [RecordBatch]s.
pub(crate) async fn stateless_multipart_put(
    data: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    file_extension: String,
    get_serializer: Box<dyn Fn() -> Box<dyn BatchSerializer> + Send>,
    config: &FileSinkConfig,
    compression: FileCompressionType,
) -> Result<u64> {
    let object_store = context
        .runtime_env()
        .object_store(&config.object_store_url)?;

    let single_file_output = config.single_file_output;
    let base_output_path = &config.table_paths[0];
    let unbounded_input = config.unbounded_input;
    let part_cols = if !config.table_partition_cols.is_empty() {
        Some(config.table_partition_cols.clone())
    } else {
        None
    };

    let (demux_task, mut file_stream_rx) = start_demuxer_task(
        data,
        context,
        part_cols,
        base_output_path.clone(),
        file_extension,
        single_file_output,
    );

    let rb_buffer_size = &context
        .session_config()
        .options()
        .execution
        .max_buffered_batches_per_output_file;

    let (tx_file_bundle, rx_file_bundle) = tokio::sync::mpsc::channel(rb_buffer_size / 2);
    let (tx_row_cnt, rx_row_cnt) = tokio::sync::oneshot::channel();
    let write_coordinater_task = tokio::spawn(async move {
        stateless_serialize_and_write_files(rx_file_bundle, tx_row_cnt, unbounded_input)
            .await
    });
    while let Some((output_location, rb_stream)) = file_stream_rx.recv().await {
        let serializer = get_serializer();
        let object_meta = ObjectMeta {
            location: output_location,
            last_modified: chrono::offset::Utc::now(),
            size: 0,
            e_tag: None,
        };
        let writer = create_writer(
            FileWriterMode::PutMultipart,
            compression,
            object_meta.into(),
            object_store.clone(),
        )
        .await?;

        tx_file_bundle
            .send((rb_stream, serializer, writer))
            .await
            .map_err(|_| {
                DataFusionError::Internal(
                    "Writer receive file bundle channel closed unexpectedly!".into(),
                )
            })?;
    }

    // Signal to the write coordinater that no more files are coming
    drop(tx_file_bundle);

    match try_join!(write_coordinater_task, demux_task) {
        Ok((r1, r2)) => {
            r1?;
            r2?;
        }
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                unreachable!();
            }
        }
    }

    let total_count = rx_row_cnt.await.map_err(|_| {
        DataFusionError::Internal(
            "Did not receieve row count from write coordinater".into(),
        )
    })?;

    Ok(total_count)
}

/// Orchestrates append_all for any statelessly serialized file type. Appends to all files provided
/// in a round robin fashion.
pub(crate) async fn stateless_append_all(
    mut data: SendableRecordBatchStream,
    context: &Arc<TaskContext>,
    object_store: Arc<dyn ObjectStore>,
    file_groups: &Vec<PartitionedFile>,
    unbounded_input: bool,
    compression: FileCompressionType,
    get_serializer: Box<dyn Fn(usize) -> Box<dyn BatchSerializer> + Send>,
) -> Result<u64> {
    let rb_buffer_size = &context
        .session_config()
        .options()
        .execution
        .max_buffered_batches_per_output_file;

    let (tx_file_bundle, rx_file_bundle) = tokio::sync::mpsc::channel(file_groups.len());
    let mut send_channels = vec![];
    for file_group in file_groups {
        let serializer = get_serializer(file_group.object_meta.size);

        let file = file_group.clone();
        let writer = create_writer(
            FileWriterMode::Append,
            compression,
            file.object_meta.clone().into(),
            object_store.clone(),
        )
        .await?;

        let (tx, rx) = tokio::sync::mpsc::channel(rb_buffer_size / 2);
        send_channels.push(tx);
        tx_file_bundle
            .send((rx, serializer, writer))
            .await
            .map_err(|_| {
                DataFusionError::Internal(
                    "Writer receive file bundle channel closed unexpectedly!".into(),
                )
            })?;
    }

    let (tx_row_cnt, rx_row_cnt) = tokio::sync::oneshot::channel();
    let write_coordinater_task = tokio::spawn(async move {
        stateless_serialize_and_write_files(rx_file_bundle, tx_row_cnt, unbounded_input)
            .await
    });

    // Append to file groups in round robin
    let mut next_file_idx = 0;
    while let Some(rb) = data.next().await.transpose()? {
        send_channels[next_file_idx].send(rb).await.map_err(|_| {
            DataFusionError::Internal(
                "Recordbatch file append stream closed unexpectedly!".into(),
            )
        })?;
        next_file_idx = (next_file_idx + 1) % send_channels.len();
        if unbounded_input {
            tokio::task::yield_now().await;
        }
    }
    // Signal to the write coordinater that no more files are coming
    drop(tx_file_bundle);
    drop(send_channels);

    let total_count = rx_row_cnt.await.map_err(|_| {
        DataFusionError::Internal(
            "Did not receieve row count from write coordinater".into(),
        )
    })?;

    match try_join!(write_coordinater_task) {
        Ok(r1) => {
            r1.0?;
        }
        Err(e) => {
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                unreachable!();
            }
        }
    }

    Ok(total_count)
}
