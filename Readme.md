**lsmt is a key-value store for the JVM, inspired by [LevelDB](https://github.com/google/leveldb)**

# Features
* Keys are `Strings`, values are arrays of bytes.
* The basic operations are `Put(key,value)`, `Get(key)`, `Delete(key)`.
* No external dependencies except Logback and Kotlin-Logging

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
* The database is not thread-safe. If you need to read and write from multiple threads, you must handle synchronization yourself.
* No client-server support.
* Currently, the LSM tree index stores key ranges in a naive way. Read and merge performance will suffer. I plan to replace this with an interval tree for more efficient queries.
* Failure recovery is not yet implemented.

# Performance
JMH benchmarks are in `src/jmh`. I have put little to no effort into profiling and tuning, but the system is architected with performance in mind.

## Latest benchmark
Benchmarks data is from my 2018 MacBook Pro with a 2.7 GHz Quad-Core Intel Core i7.
```text
Benchmark                      Mode  Cnt       Score         Error       Units
SequentialWrites.singleWrite  thrpt    2  246461.567                     ops/s
SequentialReads.singleRead    thrpt   10  1280590.622 ± 2871877.502      ops/s
```

### Synchronized - 1 reader thread at a time
```text
SequentialReads.singleRead  thrpt   10  32770.488 ± 7084.871  ops/s
```

### One reader per get()
```text
SequentialReads.singleRead  thrpt   10  96938.381 ± 18779.865  ops/s
```

### ThreadLocal reader
```text
SequentialReads.singleRead  thrpt   10  96898.472 ± 17878.384  ops/s
```

### ThreadLocal reader with improved key iteration
```text
SequentialReads.singleRead  thrpt   10  32924.280 ± 87581.672  ops/s
```

### Byte arrays for keys
```text
SequentialReads.singleRead  thrpt   10  3859520.502 ± 1459138.981  ops/s
SequentialWrites.singleWrite  thrpt    2  475170.724          ops/s
```

## Running benchmarks
Benchmarks can be run with
```bash
gradle jmh
```

# Design
The high level architecture and the binary log format were inspired by LevelDB. Storage is accomplished with a log-structured merge-tree.

# Why should I use this instead of ${other database}
I built this because I wanted to learn how LSM trees work. You probably shouldn't use it for anything!

## TODO
- Add global bloom filter for faster `get()` on nonexistent keys.
- Interval tree for index. Reads are currently bottlenecked by key range lookups.
- Move memtable off heap. This should reduce GC pressure in write-heavy workloads.
- Add more JMH benchmarks for different scenarios including random reads and writes.
