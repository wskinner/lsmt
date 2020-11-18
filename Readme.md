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
```text
Benchmark                      Mode  Cnt       Score       Error  Units
SequentialReads.singleRead    thrpt   10  195979.981 ± 55956.806  ops/s
SequentialWrites.singleWrite  thrpt    2  415899.844              ops/s
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
