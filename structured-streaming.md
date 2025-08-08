# PySpark Structured Streaming Handbook

## Table of Contents
1. [Introduction](#introduction)
2. [Core Concepts](#core-concepts)
3. [Setting Up Your Environment](#setting-up-your-environment)
4. [Data Sources](#data-sources)
5. [Basic Stream Operations](#basic-stream-operations)
6. [Window Operations](#window-operations)
7. [Watermarking](#watermarking)
8. [Output Sinks](#output-sinks)
9. [Triggers](#triggers)
10. [State Management](#state-management)
11. [Error Handling & Monitoring](#error-handling--monitoring)
12. [Performance Optimization](#performance-optimization)
13. [Common Patterns](#common-patterns)
14. [Troubleshooting Guide](#troubleshooting-guide)
15. [Best Practices](#best-practices)

---

## Introduction

Structured Streaming is Apache Spark's scalable and fault-tolerant stream processing engine built on the Spark SQL engine. It treats streaming data as an unbounded table that is continuously appended to, allowing you to use familiar DataFrame/Dataset APIs for stream processing.

### Key Benefits
- **Unified API**: Same code works for batch and streaming
- **Fault Tolerance**: Automatic recovery from failures
- **Exactly-once Processing**: Guarantees no data loss or duplication
- **Low Latency**: Sub-second latencies with micro-batch processing

---

## Core Concepts

### Streaming DataFrame
A streaming DataFrame represents an unbounded table of data that grows continuously as new data arrives.

```python
# Create a streaming DataFrame
streaming_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

### Triggers
Control when to process data:
- **Default**: As soon as previous batch completes
- **Fixed Interval**: Process every X seconds
- **One-time**: Process once and stop
- **Continuous**: Experimental low-latency mode

### Output Modes
How to write results to output sink:
- **Append**: Only new rows added to result table
- **Complete**: Entire result table written
- **Update**: Only rows that changed

---

## Setting Up Your Environment

### Basic Setup
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Create Spark Session
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingApp") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")
```

### Configuration Best Practices
```python
spark = SparkSession \
    .builder \
    .appName("StreamingApp") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.streaming.checkpointLocation", "/path/to/checkpoint") \
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
    .getOrCreate()
```

---

## Data Sources

### File Sources

#### JSON Files
```python
# Schema definition (recommended for performance)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("value", DoubleType(), True)
])

# Read streaming JSON files
json_stream = spark \
    .readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "/path/to/json/files") \
    .option("maxFilesPerTrigger", 1) \
    .load()
```

#### CSV Files
```python
csv_stream = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("path", "/path/to/csv/files") \
    .load()
```

### Kafka Source
```python
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka message
parsed_stream = kafka_stream \
    .select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("timestamp"),
        col("partition"),
        col("offset")
    )
```

### Socket Source (for testing)
```python
socket_stream = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

### Rate Source (for testing)
```python
# Generate test data
rate_stream = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .option("numPartitions", 2) \
    .load()
```

---

## Basic Stream Operations

### Filtering
```python
# Simple filter
filtered = streaming_df.filter(col("value") > 100)

# Multiple conditions
filtered = streaming_df.filter(
    (col("event_type") == "purchase") & 
    (col("value") > 50)
)
```

### Selecting and Transforming
```python
# Select specific columns
selected = streaming_df.select("user_id", "event_type", "timestamp")

# Add calculated columns
transformed = streaming_df \
    .withColumn("processed_time", current_timestamp()) \
    .withColumn("value_squared", col("value") * col("value"))
```

### Aggregations
```python
# Simple aggregation
aggregated = streaming_df \
    .groupBy("event_type") \
    .agg(
        count("*").alias("count"),
        avg("value").alias("avg_value"),
        max("value").alias("max_value")
    )
```

### Joins

#### Stream-Static Join
```python
# Load static data
static_df = spark.read.json("/path/to/static/data")

# Join streaming with static data
joined = streaming_df.join(
    static_df,
    streaming_df.user_id == static_df.id,
    "left"
)
```

#### Stream-Stream Join
```python
# Two streaming sources
stream1 = spark.readStream.format("kafka")...
stream2 = spark.readStream.format("kafka")...

# Join with watermark (required for stream-stream joins)
stream1_with_watermark = stream1 \
    .withWatermark("timestamp", "10 minutes")

stream2_with_watermark = stream2 \
    .withWatermark("timestamp", "5 minutes")

joined = stream1_with_watermark.join(
    stream2_with_watermark,
    expr("""
        stream1.user_id = stream2.user_id AND
        stream1.timestamp >= stream2.timestamp AND
        stream1.timestamp <= stream2.timestamp + interval 20 minutes
    """),
    "inner"
)
```

---

## Window Operations

### Fixed Windows
```python
# Group by fixed time windows
windowed = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("event_type")
    ) \
    .agg(count("*").alias("count"))
```

### Sliding Windows
```python
# Overlapping windows
sliding_windowed = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),  # window, slide
        col("user_id")
    ) \
    .agg(sum("value").alias("total_value"))
```

### Session Windows
```python
# Group events by session gaps
session_windowed = streaming_df \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(
        col("user_id"),
        session_window(col("timestamp"), "20 minutes")  # session gap
    ) \
    .agg(count("*").alias("events_in_session"))
```

### Window Functions
```python
from pyspark.sql.window import Window

# Ranking within windows
windowed_ranked = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .withColumn(
        "rank",
        row_number().over(
            Window.partitionBy(
                window(col("timestamp"), "1 hour"),
                col("event_type")
            ).orderBy(desc("value"))
        )
    )
```

---

## Watermarking

Watermarking handles late-arriving data by defining how late data can be accepted.

### Basic Watermarking
```python
# Allow data up to 10 minutes late
watermarked = streaming_df \
    .withWatermark("timestamp", "10 minutes")
```

### Watermarking with Aggregations
```python
# Watermark is essential for stateful operations
result = streaming_df \
    .withWatermark("event_timestamp", "1 hour") \
    .groupBy(
        window(col("event_timestamp"), "30 minutes"),
        col("user_id")
    ) \
    .agg(
        count("*").alias("event_count"),
        sum("value").alias("total_value")
    )
```

### Multiple Watermarks
```python
# When joining streams, use appropriate watermarks for each
stream1_watermarked = stream1.withWatermark("timestamp", "10 minutes")
stream2_watermarked = stream2.withWatermark("timestamp", "15 minutes")
```

---

## Output Sinks

### Console Sink (for debugging)
```python
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

query.awaitTermination()
```

### File Sink
```python
# Parquet files
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/path/to/output") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .partitionBy("date") \
    .start()

# JSON files
query = streaming_df \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/path/to/output") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

### Kafka Sink
```python
# Write to Kafka
kafka_query = processed_df \
    .select(
        col("key").cast("string"),
        to_json(struct(col("*"))).alias("value")
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

### Delta Lake Sink
```python
# Write to Delta table (if Delta Lake is available)
delta_query = streaming_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start("/path/to/delta/table")
```

### Custom Sink (foreach)
```python
def process_batch(batch_df, batch_id):
    """Custom processing for each batch"""
    print(f"Processing batch {batch_id}")
    batch_df.show()
    # Custom logic here (e.g., write to database)

query = streaming_df \
    .writeStream \
    .foreachBatch(process_batch) \
    .start()
```

### Memory Sink (for testing)
```python
query = streaming_df \
    .writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("test_table") \
    .start()

# Query the in-memory table
spark.sql("SELECT * FROM test_table").show()
```

---

## Triggers

### Processing Time Trigger
```python
# Process every 30 seconds
query = streaming_df \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .start()
```

### Once Trigger
```python
# Process once and stop
query = streaming_df \
    .writeStream \
    .trigger(once=True) \
    .format("parquet") \
    .option("path", "/path/to/output") \
    .start()
```

### Continuous Trigger (Experimental)
```python
# Low-latency continuous processing
query = streaming_df \
    .writeStream \
    .trigger(continuous='1 second') \
    .format("console") \
    .start()
```

---

## State Management

### Stateful Operations
Operations that maintain state across batches:
- Aggregations with groupBy
- Window operations
- Stream-stream joins
- Deduplication

### Managing State Size
```python
# Use watermarking to clean up old state
streaming_df \
    .withWatermark("timestamp", "1 day") \
    .dropDuplicates(["user_id", "timestamp"])
```

### Custom State with mapGroupsWithState
```python
from pyspark.sql.streaming.state import GroupState

def update_user_state(key, values, state):
    """Custom state management logic"""
    if state.hasTimedOut:
        # Handle timeout
        return []
    
    # Update state based on new values
    current_state = state.getOption or 0
    new_state = current_state + sum(v.value for v in values)
    state.update(new_state)
    
    return [Row(user_id=key, total=new_state)]

# Apply custom state function
stateful_stream = streaming_df \
    .groupByKey(lambda x: x.user_id) \
    .mapGroupsWithState(
        update_user_state,
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )
```

---

## Error Handling & Monitoring

### Query Monitoring
```python
# Start query with monitoring
query = streaming_df \
    .writeStream \
    .format("console") \
    .start()

# Monitor progress
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Batch ID: {progress['batchId']}")
        print(f"Input rows: {progress['inputRowsPerSecond']}")
        print(f"Processing time: {progress['batchDuration']} ms")
    
    time.sleep(10)
```

### Exception Handling
```python
try:
    query = streaming_df \
        .writeStream \
        .format("console") \
        .start()
    
    query.awaitTermination()
    
except Exception as e:
    print(f"Streaming query failed: {e}")
    # Implement recovery logic
    if 'query' in locals():
        query.stop()
```

### Query Status
```python
# Check query status
print(f"Query ID: {query.id}")
print(f"Run ID: {query.runId}")
print(f"Is Active: {query.isActive}")
print(f"Status: {query.status}")

# Get recent progress
recent_progress = query.recentProgress
for progress in recent_progress[-5:]:  # Last 5 batches
    print(f"Batch {progress['batchId']}: {progress['inputRowsPerSecond']} rows/sec")
```

---

## Performance Optimization

### Partitioning
```python
# Repartition for better parallelism
repartitioned = streaming_df \
    .repartition(col("user_id"))

# Coalesce to reduce small files
coalesced = streaming_df \
    .coalesce(1)
```

### Caching
```python
# Cache frequently accessed streams
cached_stream = streaming_df \
    .cache()

# Persist with specific storage level
from pyspark import StorageLevel
persisted_stream = streaming_df \
    .persist(StorageLevel.MEMORY_AND_DISK)
```

### Optimization Configurations
```python
# Key performance configurations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
spark.conf.set("spark.sql.streaming.ui.enabled", "true")
```

### Schema Optimization
```python
# Always provide schema for JSON/CSV sources
schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("timestamp", TimestampType(), False)
])

# Use schema instead of schema inference
optimized_stream = spark \
    .readStream \
    .schema(schema) \
    .json("/path/to/files")
```

---

## Common Patterns

### Event Deduplication
```python
# Remove duplicate events
deduplicated = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["event_id", "user_id"])
```

### Late Data Handling
```python
# Separate late data for special processing
current_data = streaming_df \
    .filter(col("timestamp") > current_timestamp() - expr("INTERVAL 1 HOUR"))

late_data = streaming_df \
    .filter(col("timestamp") <= current_timestamp() - expr("INTERVAL 1 HOUR"))
```

### Real-time Alerting
```python
def send_alert(batch_df, batch_id):
    """Send alerts for anomalous data"""
    alerts = batch_df.filter(col("value") > 1000).collect()
    for alert in alerts:
        print(f"ALERT: High value detected: {alert}")
        # Send notification (email, Slack, etc.)

alert_query = streaming_df \
    .writeStream \
    .foreachBatch(send_alert) \
    .start()
```

### Multi-output Pattern
```python
# Write to multiple sinks
console_query = processed_df \
    .writeStream \
    .format("console") \
    .queryName("console_output") \
    .start()

file_query = processed_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/path/to/files") \
    .queryName("file_output") \
    .start()

# Wait for all queries
spark.streams.awaitAnyTermination()
```

---

## Troubleshooting Guide

### Common Issues

#### Schema Evolution
```python
# Handle schema changes gracefully
def handle_schema_evolution(batch_df, batch_id):
    try:
        # Process with expected schema
        processed = batch_df.select("expected_column1", "expected_column2")
    except Exception as e:
        # Handle schema mismatch
        print(f"Schema mismatch in batch {batch_id}: {e}")
        # Log problematic records or apply schema fixes

query = streaming_df \
    .writeStream \
    .foreachBatch(handle_schema_evolution) \
    .start()
```

#### Memory Issues
```python
# Configure memory settings
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
spark.conf.set("spark.sql.streaming.minBatchesToRetain", "10")
```

#### Checkpoint Recovery
```python
# Clean restart after code changes
import os
checkpoint_dir = "/path/to/checkpoint"

# Remove checkpoint for clean restart (development only)
if os.path.exists(checkpoint_dir):
    import shutil
    shutil.rmtree(checkpoint_dir)
```

### Debugging Tips

#### Enable Detailed Logging
```python
# Enable streaming metrics
spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

# Set logging level
spark.sparkContext.setLogLevel("INFO")
```

#### Inspect Stream Schema
```python
# Print schema before processing
streaming_df.printSchema()

# Show sample data
sample_query = streaming_df \
    .writeStream \
    .format("console") \
    .option("numRows", 5) \
    .start()
```

---

## Best Practices

### Development
1. **Always define schemas** for JSON and CSV sources
2. **Use rate source** for testing and development
3. **Start with console sink** for debugging
4. **Use watermarking** for all stateful operations
5. **Test with small data** before scaling up

### Production
1. **Set appropriate checkpoint locations** on reliable storage
2. **Monitor query health** and set up alerting
3. **Use appropriate trigger intervals** based on latency requirements
4. **Configure resource allocation** properly
5. **Handle failures gracefully** with retry logic

### Performance
1. **Partition data appropriately** based on access patterns
2. **Use appropriate output modes** (prefer append when possible)
3. **Optimize join operations** (broadcast small tables)
4. **Monitor resource utilization** and scale accordingly
5. **Clean up old checkpoints** periodically

### Code Organization
```python
# Recommended project structure
class StreamProcessor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def create_source(self):
        """Create streaming source"""
        return self.spark.readStream...
    
    def transform(self, streaming_df):
        """Apply transformations"""
        return streaming_df.filter(...)
    
    def write_output(self, streaming_df):
        """Write to sink"""
        return streaming_df.writeStream...
    
    def run(self):
        """Main processing pipeline"""
        source = self.create_source()
        transformed = self.transform(source)
        query = self.write_output(transformed)
        return query
```

---

## Quick Reference

### Essential Imports
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
```

### Common Patterns Cheat Sheet
```python
# Basic stream setup
streaming_df = spark.readStream.format("source").load()

# Add watermark
watermarked_df = streaming_df.withWatermark("timestamp", "10 minutes")

# Window aggregation
windowed = watermarked_df.groupBy(window("timestamp", "5 minutes")).count()

# Write to sink
query = windowed.writeStream.format("console").start()

# Wait for termination
query.awaitTermination()
```

### Useful Functions
- `current_timestamp()`: Current processing time
- `window()`: Time-based windows
- `session_window()`: Session-based windows
- `to_json()`: Convert struct to JSON string
- `from_json()`: Parse JSON string to struct
- `explode()`: Flatten arrays
- `collect_list()`: Aggregate values into array

---

