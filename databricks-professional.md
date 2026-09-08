## Spark Structured Streaming — Exam Quick Table

 | Concept | Meaning | Example / Syntax | Easy exam memory |
| --- | --- | --- | --- |
| **Streaming DataFrame** | Continuously processes new data | `spark.readStream` | **Read continuously** |
| **Batch DataFrame** | Processes existing data once | `spark.read` | **Read once** |
| **Event Time** | Time when the event actually happened | `order_timestamp` | **When event happened** |
| **Processing Time** | Time when Spark processes the event | System/Spark clock | **When Spark sees it** |
| **Watermark** | Tells Spark how much **late event-time data** to tolerate; helps clean old state | `.withWatermark("order_timestamp", "10 minutes")` | **Late data + state cleanup** |
| **Window** | Groups records based on time | `window("order_timestamp", "5 minutes")` | **Time bucket** |
| **Tumbling Window** | Fixed-size, **non-overlapping** windows | `window("ts", "5 minutes")` | **No overlap** |
| **Sliding Window** | Windows that **overlap** when slide \< duration | `window("ts", "10 minutes", "5 minutes")` | **Overlap** |
| **Session Window** | Groups events separated by periods of inactivity | `session_window("ts", "10 minutes")` | **Activity → inactivity → new session** |
| **Window Duration** | How long each window covers | `10 minutes` | **Window size** |
| **Slide Duration** | How often a new window starts | `5 minutes` | **How often window moves** |
| **Duration = Slide** | Windows don't overlap | `5 min, 5 min` | **Tumbling** |
| **Duration \> Slide** | Windows overlap | `10 min, 5 min` | **Sliding** |
| **`groupBy(window, ...)`** | Groups by time window + other columns | `groupBy(window(...), "author")` | **Separate result per window + author** |
| **Aggregation** | Calculates summary values | `count()`, `avg()` | **Summarize data** |
| **Count** | Counts values/rows according to the expression | `count("order_id")` | **How many** |
| **Average** | Calculates mean | `avg("quantity")` | **Average value** |
| **`writeStream`** | Continuously writes results | `.writeStream` | **Write continuously** |
| **Checkpoint** | Stores progress/state needed for recovery | `.option("checkpointLocation", "...")` | **Recovery + state** |
| **Output: Append** | Outputs rows when they are finalized | `outputMode("append")` | **New/final rows only** |
| **Output: Update** | Outputs rows whose results changed | `outputMode("update")` | **Changed rows** |
| **Output: Complete** | Outputs the entire current result table | `outputMode("complete")` | **Everything** |
| **Trigger** | Controls when Spark processes/writes a batch of streaming data | `.trigger(...)` | **When to process** |
| **Stateful Operation** | Spark must remember previous data/state | Windowed aggregation, joins | **Spark remembers** |
| **Stateless Operation** | Each record can be processed independently | `select`, `filter`, `withColumn` | **No memory of previous records** |
| **Late Data** | Event arrives after its event time | Event says `10:02`, arrives at `10:12` | **Event happened earlier, arrived later** |
| **Sink** | Destination for streaming output | Table, files, Kafka, etc. | **Where results go** |

### ⭐ Your query in exam language

 | Code | What it means |
| --- | --- |
| `readStream` | Read continuously |
| `.table("orders_cleaned")` | Streaming source |
| `withWatermark("order_timestamp", "10 minutes")` | Allow/manage late event-time data |
| `window("order_timestamp", "5 minutes")` | **5-minute tumbling/non-overlapping window** |
| `"author"` | Group separately by author |
| `count("order_id")` | Count orders |
| `avg("quantity")` | Average quantity |
| `writeStream` | Continuously write |
| `checkpointLocation` | Recovery/progress/state |
| `.table("orders_stats")` | Output/sink table |

### 🧠 5 things to memorize first

 | If exam says... | Think... |
| --- | --- |
| **Event time** | When event happened |
| **Processing time** | When Spark processed it |
| **Watermark** | Late data + state cleanup |
| **Tumbling** | **No overlap** |
| **Sliding** | **Overlap** |

**One golden rule:**\
 `window("ts", "5 minutes")` → **tumbling**.\
 `window("ts", "10 minutes", "5 minutes")` → **sliding/overlapping**.


---

# Mind-Map | Exam 

Yes — for exam revision, the best approach is to have **one master query** and then a **tiny mind-map table** underneath it.

 ## 1\. Master example query

 This example shows the major parameters you should recognize in Spark Structured Streaming:

```
from pyspark.sql.functions import window, count, avg, session_window

query = (
    spark.readStream
        .table("orders_cleaned")

        # Event-time column + allowed lateness
        .withWatermark("order_timestamp", "10 minutes")

        # Tumbling / non-overlapping window
        .groupBy(
            window(
                "order_timestamp",
                "5 minutes",       # window duration
                "5 minutes"        # slide duration
            ).alias("time"),
            "author"
        )

        .agg(
            count("order_id").alias("orders_count"),
            avg("quantity").alias("avg_quantity")
        )

        .writeStream
        .outputMode("update")
        .trigger(processingTime="1 minute")
        .option(
            "checkpointLocation",
            "dbfs:/path/checkpoint/orders_stats"
        )
        .table("orders_stats")
)
```

 ### Same query with the important alternatives

```
SOURCE
spark.readStream.table(...)
       │
       ▼
EVENT TIME
order_timestamp
       │
       ▼
WATERMARK
10 minutes
       │
       ▼
WINDOW
       │
       ├── Tumbling:
       │   window("ts", "5 minutes")
       │
       ├── Tumbling explicitly:
       │   window("ts", "5 minutes", "5 minutes")
       │
       └── Sliding:
           window("ts", "10 minutes", "5 minutes")
       │
       ▼
GROUP BY
window + author
       │
       ▼
AGGREGATION
count + avg
       │
       ▼
OUTPUT MODE
append / update / complete
       │
       ▼
TRIGGER
processing time / available now / once
       │
       ▼
CHECKPOINT
recovery + state
       │
       ▼
SINK
orders_stats
```

 # 2\. 🧠 Spark Streaming Mind-Map Revision Table

 | Concept | Possible values / examples | Meaning — **remember this** |
| --- | --- | --- |
| **Read** | `read` / `readStream` | `read` = batch; `readStream` = streaming |
| **Source** | `table()`, files, Kafka, etc. | **Where data comes from** |
| **Event Time** | `order_timestamp` | **When event happened** |
| **Processing Time** | Spark/system clock | **When Spark processes event** |
| **Watermark** | `"10 minutes"` | **How late event-time data can be tolerated; enables old state cleanup** |
| **Window** | `"5 minutes"` | **Time bucket for aggregation** |
| **Tumbling** | `window("ts","5 min")` | **No overlap** |
| **Sliding** | `window("ts","10 min","5 min")` | **Overlap** |
| **Window duration** | `10 min` | **Size of window** |
| **Slide duration** | `5 min` | **How frequently window starts** |
| **Duration = Slide** | `5, 5` | **Tumbling / no overlap** |
| **Duration \> Slide** | `10, 5` | **Sliding / overlap** |
| **Session** | `session_window("ts","10 min")` | **Activity separated by inactivity** |
| **GroupBy** | `window + author` | **Group by time + dimension** |
| **Aggregation** | `count`, `avg`, `sum`, `max`, etc. | **Calculate summary** |
| **Stateful** | Window aggregation, streaming joins | **Spark remembers previous data** |
| **Stateless** | `filter`, `select`, simple `withColumn` | **No previous-state dependency** |
| **Output mode** | `append` | **Only new/final rows** |
| **Output mode** | `update` | **Only changed rows** |
| **Output mode** | `complete` | **Entire result table** |
| **Trigger** | `processingTime="1 minute"` | **Process every 1 minute** |
| **Trigger** | `availableNow=True` | **Process available data, then stop** |
| **Trigger** | `once=True` | **One micro-batch, then stop** |
| **Trigger** | default | **Process as soon as possible** |
| **writeStream** | `writeStream` | **Write continuously** |
| **Sink** | table/files/Kafka/etc. | **Where results go** |
| **Checkpoint** | `checkpointLocation` | **Recovery + progress + state** |
| **Late data** | Event arrives after event time | **Watermark deals with it** |

 ## 3\. The 10-second exam memory map

```
                 SPARK STREAMING
                       │
       ┌───────────────┼────────────────┐
       ▼               ▼                ▼
     TIME            WINDOW           STATE
       │               │                │
 ┌─────┴─────┐    ┌────┴─────┐     Watermark
 │           │    │          │          │
Event     Processing Tumbling Sliding  Late data
time       time       │       │         │
 │                    │       │         └─ cleanup
When it               │       │
happened              │       └─ overlap
                       └─ no overlap

                       │
                       ▼
                  AGGREGATION
                       │
                count / avg / sum
                       │
                       ▼
                  OUTPUT MODE
                ┌──────┼──────┐
              Append  Update Complete
                │       │       │
             final    changed  everything

                       │
                       ▼
                    TRIGGER
             when should process?
                       │
                       ▼
                  CHECKPOINT
             recovery + state
                       │
                       ▼
                     SINK
               where to write?
```

 ### ⭐ Absolute must-know distinctions

 | Don't confuse | Correct distinction |
| --- | --- |
| **Event time vs Processing time** | Event = when it happened; Processing = when Spark processes it |
| **Window vs Watermark** | Window = grouping; Watermark = late-data/state management |
| **Tumbling vs Sliding** | Tumbling = no overlap; Sliding = overlap |
| **Window duration vs Slide** | Duration = window size; Slide = window start frequency |
| **Checkpoint vs Sink** | Checkpoint = recovery/state; Sink = actual output |
| **Append vs Update vs Complete** | Final new rows vs changed rows vs entire result |
| **Stateless vs Stateful** | No memory needed vs Spark maintains state |

 **Exam shortcut:** Whenever you see `withWatermark + window + groupBy + agg`, immediately think **STATEFUL STREAMING AGGREGATION**. The watermark is there primarily to control late data and allow Spark to eventually remove old state.
