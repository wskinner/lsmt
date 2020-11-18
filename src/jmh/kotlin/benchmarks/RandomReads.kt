package benchmarks

import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.cache.StandardKeyIterator
import com.lsmt.toByteArray
import com.lsmt.toKey
import org.openjdk.jmh.annotations.*
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * This benchmark attempts to measure the pure read performance of the system on a random set of records. Records
 * are inserted in increasing order by key. Records are accessed by sampling from the key range.
 *
 * Each record has a key of 16 bytes and a variable-size value of 100 bytes.
 *
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
@Measurement(iterations = 10)
@Fork(1)
@Warmup(iterations = 1)
open class RandomReads {
    private var tree: LogStructuredMergeTree? = null

    @State(Scope.Thread)
    open class ThreadState {
        private val currentThread = Thread.currentThread().id
        val keyIterator: Iterator<Key> = randomKeySequence(keyRangeSize, currentThread).iterator()
    }

    @Setup
    fun setup() {
        tree = treeFactory().apply { fillTree(keyRangeSize, valueSize) }
        // Synchronize on the completion of compaction and SSTable creation.
        tree?.close()
    }

    @TearDown
    fun metrics() {
        println(
            "totalSeekBytes=${StandardKeyIterator.totalSeekBytes.get()} totalReads=${StandardKeyIterator.totalReads.get()} averageSeekBytes = ${
                StandardKeyIterator.totalSeekBytes.get().toDouble() / StandardKeyIterator.totalReads.get()
            }"
        )
    }

    @Benchmark
    fun singleRead(state: ThreadState): Record? {
        return tree!!.get(state.keyIterator.next())
    }

    companion object {
        const val keyRangeSize = 10_000_000
        const val valueSize = 100
    }
}

fun randomKeySequence(max: Int, seed: Long) = sequence {
    val random = Random(seed)
    while (true) {
        val key = (random.nextLong() % max)
            .toByteArray(littleEndian = false)
            .toKey()
        yield(key)
    }
}
