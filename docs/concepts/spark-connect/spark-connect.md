# Spark Connect

Spark Connect is the client-server architecture for Apache Spark. Introduced in **Spark 3.4 (April 2023)**, matured in **3.5 (Sept 2023)** and **4.0 (May 2025)**.

It decouples the client application from the Spark driver. The client became a thin library; the driver runs as a remote server.

![Spark Connect Architecture](./spark-connect-architecture.png)

---

## The problem before Spark Connect

The classic Spark architecture is monolithic. The driver, the user code, and the dependencies all live in the same JVM process.

This creates real problems:

- **Heavy client**: PySpark requires the full JVM (~355 MB). You can't embed Spark in a lightweight application.
- **Tight coupling**: a memory leak in user code can kill the driver and the whole job.
- **Single Spark version per cluster**: every client connected to the cluster has to use the same Spark and dependency versions.
- **Limited debugging**: you can't attach a local IDE to a remote driver running in the cluster.
- **No multi-tenancy**: each application needs its own dedicated cluster.

## How Spark Connect works

The architecture is split in two:

```
[Client]  ──(unresolved logical plan via gRPC)──▶  [Spark Connect Server]
                                                          │
                                                          ▼
                                                    Catalyst, Tungsten,
                                                    Executors
[Client]  ◀──(results via Apache Arrow)─────────────┘
```

### 1. The client builds an unresolved logical plan

When you write:

```python
df = spark.read.parquet("s3://bucket/data/")
result = df.filter(df.amount > 100).groupBy("category").count()
```

The client doesn't execute anything. It just builds a representation of the operations — an **unresolved logical plan** serialized as Protocol Buffers.

The plan is "unresolved" because the client doesn't know the schema, doesn't validate columns, doesn't optimize. It's just intent.

### 2. The plan travels via gRPC

The serialized plan is sent over the network using **gRPC** — a Google protocol that uses HTTP/2 and binary serialization. It's fast, language-agnostic, and bidirectional.

### 3. The server executes

The Spark Connect Server receives the plan and runs the full pipeline:

- **Analyzer**: resolves columns, types, references
- **Catalyst Optimizer**: applies logical and physical optimizations
- **Tungsten Engine**: generates code and manages memory
- **Executors**: distributes work across the cluster

### 4. Results return via Apache Arrow

The result is serialized in **Apache Arrow** — a columnar in-memory format. Arrow is much faster than row-by-row serialization (CSV, JSON) and the format is the same in any language (Python, R, Java, Go).

The client receives the Arrow data and reconstructs the DataFrame locally.

## Practical benefits

### Lightweight client

The `pyspark-connect` package is **~1.5 MB**, versus ~355 MB for full PySpark. No local JVM required.

```bash
pip install pyspark-connect
```

### Connecting to a remote cluster

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .remote("sc://spark-cluster.company.com:15002") \
    .getOrCreate()

df = spark.read.parquet("s3://bucket/data/")
df.show()
```

That's it. The code runs locally, the processing runs in the cluster.

### Isolated failures

If your client crashes (OOM, exception, ctrl+C), the cluster keeps running. Other users connected to the same server aren't affected.

In classic Spark, a crash in user code can take the driver down and restart the entire job.

### Multi-language without rewriting the server

Today there are official clients for **Python and Scala**. The community already has clients for **Go, Rust, Swift, .NET**. Anyone can implement a client — just speak the gRPC + Protobuf protocol.

The server stays the same. There's no need to maintain N implementations of Catalyst, Tungsten and the optimizer.

### Independent upgrades

The server runs Spark 4.0; the client can stay on 3.5. As long as the gRPC contract holds, both communicate without breaking.

This is critical in corporate environments where upgrading the cluster is hard.

### Real remote debugging

You can run **VS Code or IntelliJ** on your local machine, set breakpoints in the client code, and watch the queries run in the remote cluster. No SSH, no port forwarding, no embedded Jupyter.

## Where it's already in production

- **Databricks Connect v2**: rebuilt on top of Spark Connect. The old version required matching client-server library versions; v2 is decoupled.
- **Databricks Serverless Compute**: only works because of Spark Connect. The user submits queries from a thin client; Databricks manages the cluster pool transparently.
- **Spark 4.0**: a release tarball ships with Spark Connect enabled by default.

## When to use it

**Use it when:**
- You need to embed Spark in a backend application or microservice
- You want to develop locally with VS Code/IntelliJ against a remote cluster
- You need multi-tenancy on a single cluster
- You want to use non-JVM languages (Go, Rust)
- You want to upgrade the cluster without breaking applications

**You don't need to migrate when:**
- You only use Databricks notebooks (it's already abstracted)
- Your workload is a scheduled batch job and the current monolithic model is fine

## Limitations (as of Spark 4.0)

- **RDD API not supported**: only DataFrame/Dataset. If you use `sc.parallelize(...)`, you'd have to rewrite.
- **SparkContext not exposed**: low-level operations on the context don't work in the client.
- **Some UDFs need adjustment**: especially those that depend on local JVM state.

## References

- [Spark Connect Overview - Apache Spark Docs](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark 3.4 Release Notes](https://spark.apache.org/releases/spark-release-3-4-0.html)
- [Databricks: Spark Connect GA Announcement](https://www.databricks.com/blog/2023/04/18/spark-connect-available-apache-spark.html)

