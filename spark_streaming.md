The important thing is to build a **mental model of how streaming works end-to-end**, especially **Structured Streaming + checkpoints + triggers + output modes + watermarks + stateful operations + Auto Loader + Delta**.

# ⚡ Spark Structured Streaming — Exam Mind Map

Think of streaming as:

**SOURCE → READ → TRANSFORM → STATE → WRITE → CHECKPOINT**

```text
                    SPARK STREAMING
                          │
        ┌─────────────────┼─────────────────┐
        ↓                 ↓                 ↓
      SOURCE          PROCESSING          SINK
        │                 │                 │
   Auto Loader       Transformations      Delta
   Kafka              Aggregation         Kafka
   Files              Join                Console
   Delta              Watermark
                      State
                          │
                    CHECKPOINT
                          │
                 Recovery + Exactly Once
```

---

# 1. What is Structured Streaming?

Spark Structured Streaming lets you process **continuously arriving data** using the same DataFrame/Dataset APIs used for batch processing.

### Core idea

Instead of:

```text
Read existing files → process → stop
```

Streaming does:

```text
New data arrives
      ↓
Spark detects it
      ↓
Process new data
      ↓
Write results
      ↓
Wait for next trigger
      ↓
Repeat
```

### Important exam concept

**Structured Streaming is micro-batch by default.**

Think:

> **Streaming = continuously running query over an unbounded table**

---

# 2. Source

The source determines **where streaming data comes from**.

Common Databricks sources:

| Source      | Important concept                 |
| ----------- | --------------------------------- |
| Auto Loader | Incrementally discovers new files |
| Delta Lake  | Stream from Delta table           |
| Kafka       | Message/event streaming           |
| Files       | File-based streaming              |
| Rate        | Testing/demo                      |

### Most important for Databricks

**Auto Loader**

```python
spark.readStream.format("cloudFiles")
```

Auto Loader is designed for **incremental file ingestion from cloud storage**.

Mental model:

```text
Cloud Storage
     ↓
Auto Loader
     ↓
New files only
     ↓
Spark DataFrame
```

---

# 3. `read` vs `readStream`

This is extremely important.

### Batch

```python
df = spark.read.format("json").load(path)
```

Means:

> Read the data that exists **now**.

### Streaming

```python
df = spark.readStream.format("json").load(path)
```

Means:

> Keep looking for **new data**.

Remember:

```text
read       → Batch
readStream → Streaming
```

---

# 4. Transformation

Once you have a streaming DataFrame, you can perform many normal DataFrame transformations.

```python
stream_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .load(path)
)

result = stream_df.filter("amount > 100")
```

Common transformations:

```text
filter
select
withColumn
join
groupBy
aggregation
dropDuplicates
```

But some operations are **stateful** and therefore require additional concepts.

---

# 5. Stateless vs Stateful

This is a **high-value exam distinction**.

## Stateless

Each record can be processed independently.

```text
Record A → process A
Record B → process B
Record C → process C
```

Examples:

```python
filter()
select()
withColumn()
```

No historical information is required.

---

## Stateful

Spark needs to remember information from **previous records/batches**.

Examples:

```text
groupBy + aggregation
stream-stream joins
dropDuplicates
windowed aggregations
```

Mental model:

```text
New data
   ↓
Existing state
   ↓
Update state
   ↓
Output
```

This is where **checkpointing** becomes especially important.

---

# 6. Checkpointing ⭐⭐⭐

One of the most important streaming concepts.

A checkpoint stores information about the progress/state of a streaming query.

Think:

```text
Streaming Query
       ↓
Checkpoint
       ↓
"Where did I stop?"
"What state did I have?"
```

If the job crashes:

```text
Job crashes
    ↓
Restart
    ↓
Read checkpoint
    ↓
Recover progress/state
    ↓
Continue
```

### Why checkpoint?

Primarily for:

* Fault tolerance
* Recovery
* Tracking processed data
* Maintaining state

Example:

```python
.writeStream \
.option("checkpointLocation", "/checkpoint/orders") \
...
```

### Exam rule

**Each streaming query should have its own checkpoint location.**

Don't casually reuse the same checkpoint location for unrelated streaming queries.

---

# 7. Trigger

Trigger determines **when Spark processes available data**.

Think:

```text
Trigger = "When should I run?"
```

### Common trigger types

#### Default

Spark processes data as soon as possible using micro-batches.

#### Processing time

```python
.trigger(processingTime="10 seconds")
```

Means:

> Run a micro-batch every 10 seconds.

#### Available-now style

```python
.trigger(availableNow=True)
```

Important idea:

> Process all data that is currently available, then stop.

Very useful for **incremental batch-style processing**.

---

# 8. Output Modes ⭐⭐⭐

Output mode determines **what Spark writes from the streaming result**.

There are three:

```text
APPEND
UPDATE
COMPLETE
```

## Append

Only **new rows** are written.

```text
Existing output
      +
New rows
      ↓
Write only new rows
```

Common for simple transformations.

---

## Update

Only rows whose values **changed** are written.

Particularly useful with aggregations.

```text
Existing state
     ↓
New data
     ↓
Updated aggregation
     ↓
Write changed rows
```

---

## Complete

Writes the **entire result table** every trigger.

Think:

```text
Full current result
        ↓
Rewrite entire result
```

Can be expensive for large aggregations.

### Memory trick

```text
APPEND   → New
UPDATE   → Changed
COMPLETE  → Everything
```

---

# 9. Watermark ⭐⭐⭐⭐⭐

This is probably one of the most important streaming concepts for the exam.

Problem:

### Events can arrive late.

Example:

```text
10:01 event
10:02 event
10:10 event
10:03 event ← VERY LATE
```

Spark needs to know:

> How long should I wait for late events?

That's what **watermarking** helps with.

```python
.withWatermark("event_time", "10 minutes")
```

Meaning roughly:

> Spark can eventually consider data older than the watermark threshold as too late for maintaining state.

---

# 10. Why Watermark?

Main purposes:

### 1. Handle late-arriving data

### 2. Limit state

Without watermark:

```text
State
 ↓
keeps growing
 ↓
memory problem
```

With watermark:

```text
Old state
 ↓
can eventually be removed
 ↓
bounded state
```

### Mental model

```text
EVENT TIME
    ↓
Watermark
    ↓
How late can data reasonably arrive?
    ↓
Remove old state
```

---

# 11. Event Time vs Processing Time ⭐⭐⭐

Another important distinction.

### Event time

When the event **actually happened**.

```text
event_time = 10:01
```

### Processing time

When Spark **processed the event**.

```text
processing_time = 10:10
```

Example:

```text
Event happened:       10:01
Network delay
       ↓
Arrives at Spark:     10:08
       ↓
Processing time:      10:09
```

For event-time analytics, use:

```text
event_time
```

not processing time.

---

# 12. Windowing ⭐⭐⭐

Windowing groups streaming events based on time.

Example:

```text
10:00 ───── 10:05
10:05 ───── 10:10
10:10 ───── 10:15
```

Common windows:

### Tumbling window

Non-overlapping.

```text
10:00 ── 10:05
10:05 ── 10:10
10:10 ── 10:15
```

### Sliding window

Overlapping.

```text
10:00 ── 10:10
       10:05 ── 10:15
              10:10 ── 10:20
```

### Session window

Groups events based on periods of activity separated by inactivity.

```text
events → events → events
                  ↓
               inactivity
                  ↓
               new session
```

---

# 13. Watermark + Window = ⭐⭐⭐⭐⭐

These concepts frequently go together.

Example:

```python
df.withWatermark("event_time", "10 minutes") \
  .groupBy(
      window("event_time", "5 minutes")
  ) \
  .count()
```

Mental model:

```text
Events
  ↓
Event time
  ↓
Watermark
  ↓
Window
  ↓
Aggregation
  ↓
State
  ↓
Output
```

---

# 14. Streaming Aggregation

Example:

```python
df.groupBy("customer_id").count()
```

Spark must remember:

```text
customer A → 100
customer B → 50
customer C → 75
```

When new events arrive:

```text
customer A → +1

State:
customer A → 101
```

Therefore:

**Aggregation = stateful operation**

And therefore:

**Checkpointing is important.**

---

# 15. Streaming Joins

You can join streaming data.

Two major cases:

```text
Streaming + Static
Streaming + Streaming
```

## Streaming + Static

Example:

```text
Streaming orders
       +
Static customer table
       ↓
Enriched orders
```

Generally simpler.

---

## Streaming + Streaming ⭐

Example:

```text
Orders stream
       +
Payments stream
       ↓
Join
```

Harder because Spark must retain state from both streams.

This is where concepts like:

```text
watermark
time constraints
state management
```

become important.

---

# 16. Deduplication

Streaming data can contain duplicate events.

Example:

```text
order_101
order_102
order_101  ← duplicate
```

You can use:

```python
dropDuplicates(["order_id"])
```

For streaming deduplication, Spark may need to maintain state about previously seen IDs.

Therefore:

```text
Streaming deduplication
        ↓
Stateful
        ↓
Checkpoint/state management
```

Watermarks can help bound the amount of state in appropriate event-time deduplication patterns.

---

# 17. Sink / `writeStream`

After processing:

```text
Source
  ↓
Transform
  ↓
writeStream
  ↓
Sink
```

Example:

```python
query = (
    df.writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint)
      .start(output_path)
)
```

Common sinks:

```text
Delta
Kafka
Console
Memory
```

For Databricks DE exam:

**Delta is extremely important.**

---

# 18. Streaming into Delta

Typical Databricks architecture:

```text
Cloud Storage
     ↓
Auto Loader
     ↓
Bronze Delta
     ↓
Streaming transformation
     ↓
Silver Delta
     ↓
Gold Delta
```

This is the classic **Medallion + Streaming** pattern.

---

# 19. Auto Loader ⭐⭐⭐⭐⭐

Know these concepts.

```python
cloudFiles
```

Auto Loader incrementally processes new files.

Important options/concepts:

```text
cloudFiles.format
cloudFiles.schemaLocation
cloudFiles.inferColumnTypes
cloudFiles.schemaEvolutionMode
```

### `schemaLocation`

Stores schema information used by Auto Loader.

Think:

```text
New files
    ↓
Auto Loader
    ↓
Schema tracking
    ↓
Process
```

### Why Auto Loader?

Instead of repeatedly listing a huge directory:

```text
10 million files
      ↓
"Which files are new?"
```

Auto Loader is designed to efficiently discover and ingest newly arriving files.

---

# 20. Schema Evolution

Streaming pipelines need to handle schema changes.

Example:

Initial:

```text
id
name
```

Later:

```text
id
name
email
```

Auto Loader can support schema evolution depending on configuration.

Remember:

```text
New column
   ↓
Schema evolution
```

But **schema evolution behavior depends on the configured mode**—don't assume every schema change is automatically accepted.

---

# 21. Exactly Once ⭐⭐⭐⭐⭐

For exam purposes, understand the concept rather than memorizing slogans.

Streaming systems can provide strong processing guarantees through:

```text
Checkpointing
+
Idempotent/transactional sink
+
Source progress tracking
```

With Delta Lake, the combination is particularly powerful.

Mental model:

```text
Source
  ↓
Checkpoint
  ↓
Spark processing
  ↓
Delta transaction
  ↓
Reliable recovery
```

### Important distinction

**Exactly-once processing is not simply "because Spark Streaming is exactly once."**

The overall guarantee depends on the **source, processing, and sink**.

---

# 22. `foreachBatch` ⭐⭐⭐⭐

Very important Databricks concept.

`foreachBatch` allows you to treat **each micro-batch as a normal DataFrame**.

```python
def process_batch(batch_df, batch_id):
    # normal DataFrame logic
    pass

df.writeStream.foreachBatch(process_batch)
```

Mental model:

```text
Streaming Data
      ↓
Micro-batch
      ↓
foreachBatch()
      ↓
Normal batch DataFrame
```

Useful for:

* MERGE operations
* Custom writes
* Multiple sinks
* Logic unsupported directly by streaming APIs

---

# 23. `foreach`

Difference:

```text
foreachBatch → process entire micro-batch
foreach       → process individual records
```

Memory trick:

```text
foreachBatch → Batch level
foreach       → Row/record level
```

---

# 24. Stream-Static vs Stream-Stream

Remember this table:

|            | Stream + Static     | Stream + Stream   |
| ---------- | ------------------- | ----------------- |
| Complexity | Lower               | Higher            |
| State      | Usually less        | More              |
| Watermark  | Not always required | Often important   |
| Example    | Orders + Customer   | Orders + Payments |

---

# 25. Available Now vs Continuous Processing

For the exam, remember:

### Micro-batch

```text
Data → Batch 1 → Batch 2 → Batch 3
```

Default Structured Streaming model.

### Available Now

```text
Process currently available data
             ↓
Stop
```

Useful for incremental processing.

### Continuous processing

Designed for very low latency use cases, but has different execution/feature characteristics and is much less central than micro-batch for modern Databricks exam questions.

---

# 🧠 THE BIG EXAM MIND MAP

If you remember only one thing, remember this:

```text
                 STRUCTURED STREAMING
                         │
        ┌────────────────┼─────────────────┐
        │                │                 │
      SOURCE          PROCESS             SINK
        │                │                 │
        │                │                 ├── Delta
        │                │                 ├── Kafka
        │                │                 └── Console
        │                │
        ├── Auto Loader  ├── Stateless
        ├── Kafka        │     ├── filter
        ├── Delta        │     ├── select
        └── Files        │     └── withColumn
                         │
                         └── Stateful
                              ├── aggregation
                              ├── joins
                              ├── deduplication
                              └── windows
                                      │
                                      ↓
                                 WATERMARK
                                      │
                                      ↓
                                  STATE
                                      │
                                      ↓
                               CHECKPOINT
                                      │
                                      ↓
                                  RECOVERY

          READ                         WRITE
            │                            │
       readStream                    writeStream
            │                            │
            └────────────┬───────────────┘
                         ↓
                       TRIGGER
                         │
              ┌──────────┼───────────┐
              ↓          ↓           ↓
           Default   Processing   AvailableNow
                         Time
                         
                    OUTPUT MODE
                         │
               ┌─────────┼─────────┐
               ↓         ↓         ↓
            Append     Update   Complete
```

# 🔥 Exam Priority — What to Memorize

If your preparation time is limited, prioritize in this order:

### Tier 1 — MUST KNOW

1. **`readStream` vs `read`**
2. **`writeStream`**
3. **Checkpoint**
4. **Watermark**
5. **Output modes**
6. **Triggers**
7. **Auto Loader**
8. **Stateful vs stateless**
9. **Event time vs processing time**
10. **Windowing**

### Tier 2 — VERY IMPORTANT

11. Streaming aggregations
12. Stream-static joins
13. Stream-stream joins
14. Deduplication
15. `foreachBatch`
16. Schema evolution
17. Exactly-once concepts

### Tier 3 — Know conceptually

18. Continuous processing
19. `foreach`
20. Different streaming sinks
21. State-store concepts
22. Late-arriving data behavior

---

# 🎯 One-line memory sheet

```text
readStream     = continuously read
writeStream    = continuously write
Trigger        = WHEN to process
Output Mode    = WHAT to output
Checkpoint     = WHERE progress/state is remembered
Watermark      = HOW LONG to tolerate late data
State          = WHAT Spark remembers
Window         = HOW events are grouped by time
Event Time     = WHEN event happened
Processing Time= WHEN Spark processed it
Auto Loader    = incrementally discover cloud files
foreachBatch   = treat each micro-batch as DataFrame
Append         = new rows
Update         = changed rows
Complete       = entire result
Stateless      = no history needed
Stateful       = history/state needed
```

**The single biggest mental model:**

> **Streaming is a continuously running query. Stateful operations require state; checkpoints preserve progress/state; watermarks control late data and help clean old state; triggers control when processing happens; output modes control what gets emitted; Auto Loader is Databricks' key file-ingestion mechanism.**
