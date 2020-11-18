package benchmarks

import benchmarks.SequentialReads.Companion.keyRangeSize
import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.Record
import org.openjdk.jmh.annotations.*
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * This benchmark attempts to measure the pure read performance of the system on a contiguous set of records. Records
 * are accessed in increasing order by key. Since records with adjacent keys are usually stored in the same block in the
 * same SSTable file, this gives the system an opportunity for performance optimization.
 *
 * Each record has a key of 16 bytes and a variable-size value of less than 400 bytes.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
@Measurement(iterations = 1)
@Fork(1)
@Warmup(iterations = 1)
open class SequentialReads {
    private var tree: LogStructuredMergeTree? = null
    private var keyIterator: Iterator<String>? = null

    @Setup
    fun setup() {
        tree = treeFactory().apply { fillTree(keySeq()) }
        // Synchronize on the completion of compaction and SSTable creation.
        tree?.close()
        keyIterator = keySeq().iterator()
    }

    @Benchmark
    fun singleRead(): Record? {
        return tree!!.get(keyIterator!!.next())
    }

    companion object {
        const val keyRangeSize = 10_000_000
    }
}

fun keySeq(): Sequence<String> = sequence {
    var i = 0L
    while (i >= 0) {
        val k = i % keyRangeSize
        yield("abcdef$k")
        i++
    }
}

fun LogStructuredMergeTree.fillTree(keySeq: Sequence<String>) {
    val random = Random(0)

    for (key in keySeq.take(keyRangeSize)) {
        val value = TreeMap<String, Any>()
        val randomBytes = ByteArray(100)
        random.nextBytes(randomBytes)
        value["key0"] = 1
        value["key1"] = 10F
        value["key2"] = 100L
        value["key3"] = 1000.123
        value["key4"] = Base64.getEncoder().encodeToString(randomBytes)
        put(key, value)
    }
}
