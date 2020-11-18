**lsmt is a key-value store for the JVM, inspired by [LevelDB](https://github.com/google/leveldb)**

# Features
* Keys and values are arrays of bytes.
* The basic operations are `Put(key,value)`, `Get(key)`, `Delete(key)`.
* No external dependencies except Logback and Kotlin-Logging
* Each write is synced to the OS. Relaxing this requirement would improve performance.

# Building
This project is built with Gradle, and has been tested with Gradle 6.7 on OSX. It probably works with Gradle 5 as well.
## Build
```bash
gradle build
```

# Testing
```bash
gradle test
```

# Limitations
* The database is not thread-safe for writes. If you need to read and write from multiple threads, you must handle synchronization yourself.
* No client-server support.
* Failure recovery is not yet implemented.

# Performance
JMH benchmarks are in `src/jmh`. I have put little to no effort into profiling and tuning, but the system is architected with performance in mind.

## Latest benchmark (4-core Macbook Pro, 4 threads for reads)
```text
Benchmark                      Mode  Cnt        Score        Error  Units
RandomReads.singleRead        thrpt   10   329727.983 ± 136288.137  ops/s
RandomWrites.singleWrite      thrpt   10   288315.945 ± 287201.198  ops/s
SequentialReads.singleRead    thrpt   10  2814174.762 ± 558886.383  ops/s
SequentialWrites.singleWrite  thrpt   10  1223974.229 ±  74163.247  ops/s
```

## Latest benchmark (12-core desktop, 4 threads for reads)
```text
Benchmark                      Mode  Cnt        Score         Error  Units
RandomReads.singleRead        thrpt   10   352910.794 ±    3800.808  ops/s
RandomWrites.singleWrite      thrpt   10   406042.859 ±  140205.783  ops/s
SequentialReads.singleRead    thrpt   10  4590484.075 ± 1641626.408  ops/s
SequentialWrites.singleWrite  thrpt   10  1363217.373 ±   28903.651  ops/s
```

## Latest benchmark (12-core desktop, 12 threads for reads)
```text
Benchmark                      Mode  Cnt         Score         Error  Units
RandomReads.singleRead        thrpt   10    330679.687 ±    5145.566  ops/s
RandomWrites.singleWrite      thrpt   10    420988.581 ±  122476.148  ops/s
SequentialReads.singleRead    thrpt   10  11060759.012 ± 6665679.961  ops/s
SequentialWrites.singleWrite  thrpt   10   1391314.466 ±   31009.056  ops/s
```

## Latest benchmark (12-core desktop, 24 threads for reads)
```text
Benchmark                      Mode  Cnt        Score         Error  Units
RandomReads.singleRead        thrpt   10   306911.379 ±   14335.331  ops/s
RandomWrites.singleWrite      thrpt   10   406846.008 ±  262228.534  ops/s
SequentialReads.singleRead    thrpt   10  8030085.330 ± 9272832.664  ops/s
SequentialWrites.singleWrite  thrpt   10  1391049.433 ±   82424.862  ops/s
```

## Running benchmarks
Benchmarks can be run with
```bash
gradle jmh
```

# Design
The high level architecture and the binary log format were inspired by LevelDB. Storage is accomplished with a log-structured merge-tree.

# Why should I use this instead of ${other database}
I built this because I wanted to learn how LSM trees work. You probably shouldn't use it for anything.

## TODO
- Add global bloom filter for faster `get()` on nonexistent keys.
- Move memtable off heap. This should reduce GC pressure in write-heavy workloads.
- Disaster recovery
