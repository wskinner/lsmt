package benchmarks

import benchmarks.SequentialReads.Companion.keyRangeSize
import com.lsmt.core.Key
import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.Record
import com.lsmt.core.longBytesSeq
import com.lsmt.table.StandardTableIterator
import com.lsmt.toKey
import org.openjdk.jmh.annotations.*
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * This benchmark attempts to measure the pure read performance of the system on a contiguous set of records. Records
 * are accessed in increasing order by key. Since records with adjacent keys are usually stored in the same block in the
 * same SSTable file, this gives the system an opportunity for performance optimization.
 *
 * Each record has a key of 16 bytes and a variable-size value of 100 bytes.
 *
 * This benchmark produces a lot of garbage during warmup, making the result unreliable. Sometimes the first few
 * iterations are slow, presumably due to GC overhead.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(4)
@Measurement(iterations = 10)
@Fork(1)
@Warmup(iterations = 1)
open class SequentialReads {
    private var tree: LogStructuredMergeTree? = null

    @State(Scope.Thread)
    open class ThreadState {
        val keyIterator: Iterator<Key> = keySeq().iterator()
    }

    @Setup
    fun setup() {
        tree = treeFactory().apply { fillTree(keySeq()) }
        // Synchronize on the completion of compaction and SSTable creation.
        tree?.close()
    }

    @TearDown
    fun metrics() {
        println(
            "totalSeekBytes=${StandardTableIterator.totalSeekBytes.get()} totalReads=${StandardTableIterator.totalReads.get()} averageSeekBytes = ${
                StandardTableIterator.totalSeekBytes.get().toDouble() / StandardTableIterator.totalReads.get()
            }"
        )
    }

    @Benchmark
    fun singleRead(state: ThreadState): Record? {
        return tree!!.get(state.keyIterator.next())
    }

    companion object {
        const val keyRangeSize = 10_000_000
    }
}

fun keySeq() = longBytesSeq().map { it.toKey() }

fun LogStructuredMergeTree.fillTree(keySeq: Sequence<Key>) {
    val random = Random(0)

    for (key in keySeq.take(keyRangeSize)) {
        val value = ByteArray(100).apply {
            random.nextBytes(this)
        }

        put(key, value)
    }
}
