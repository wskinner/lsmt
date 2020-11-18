**lsmt is a key-value store for the JVM, inspired by [LevelDB](https://github.com/google/leveldb)**

# Features
* Keys are `Strings`, values are `Maps` of primitives.
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

# Performance
JMH benchmarks are in `src/jmh`. I have put little to no effort into profiling and tuning, but the system is architected with performance in mind. 

## Latest benchmark
```text
Benchmark                      Mode  Cnt       Score       Error  Units
SequentialWrites.singleWrite  thrpt    5  194342.643 ± 48739.356  ops/s
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