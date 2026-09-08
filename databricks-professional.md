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
