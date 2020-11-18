package benchmarks

import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.toByteArray
import com.lsmt.toKey
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Mode.Throughput
import java.util.*
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
open class SequentialWrites {
    private var counter = 0

    private val random = Random(0)
    private var tree: LogStructuredMergeTree? = null

    @Setup
    fun setup() {
        tree = treeFactory()
    }

    @TearDown
    fun teardown() {
        tree?.close()
        counter = 0
    }

    @Benchmark
    fun singleWrite() {
        val value = ByteArray(random.nextInt(1000) + 5).apply {
            random.nextBytes(this)
        }
        tree?.put(counter++.toByteArray(littleEndian = false).toKey(), value)
    }
}
