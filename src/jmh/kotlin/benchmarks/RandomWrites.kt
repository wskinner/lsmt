package benchmarks

import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.domain.Entry
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Mode.Throughput
import java.util.concurrent.TimeUnit


/**
 * This benchmark attempts to measure the write throughput with sequential keys.
 */
@State(Scope.Thread)
@BenchmarkMode(Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Measurement(iterations = 4)
@Fork(1)
@Warmup(iterations = 1)
open class RandomWrites {
    private var tree: LogStructuredMergeTree? = null
    private val iterator: Iterator<Entry> = entrySeq(keyRangeSize, valueSize)
        .shuffled()
        .repeat(100)
        .iterator()

    @Setup
    fun setup() {
        tree = treeFactory()
    }

    @TearDown
    fun teardown() {
        tree?.close()
    }

    @Benchmark
    fun singleWrite() {
        val next = iterator.next()
        tree?.put(next.first, next.second!!)
    }

    companion object {
        const val keyRangeSize = 10_000_000
        const val valueSize = 100
    }
}
