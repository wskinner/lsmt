package benchmarks

import benchmarks.SequentialWrites.Companion.maxIterations
import com.lsmt.core.LogStructuredMergeTree
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Mode.Throughput
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.Options
import org.openjdk.jmh.runner.options.OptionsBuilder
import java.util.*
import java.util.concurrent.TimeUnit


/**
 * This benchmark attempts to measure the write throughput with sequential keys.
 */
@State(Scope.Thread)
@BenchmarkMode(Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
open class SequentialWrites {
    private var counter = 0
    private val resetInterval = 1_000_000

    private val random = Random(0)
    private var tree: LogStructuredMergeTree? = null

    @Setup
    fun setup() {
        tree = treeFactory()
    }

    @TearDown
    fun teardown() {
        tree = null
        counter = 0
    }

    private fun rotate() {
        tree = treeFactory()
    }

    @Benchmark
    fun singleWrite() {
        // JMH seems to do a couple of extra iterations
        if (counter % resetInterval == 0) {
            rotate()
        } else {
            val name = ByteArray(random.nextInt(1000) + 5).run {
                random.nextBytes(this)
                Base64.getEncoder().encodeToString(this)!!
            }
            tree?.put(
                "person$counter", sortedMapOf(
                    "name" to name,
                    "age" to random.nextInt()
                )
            )
        }

        counter++
    }

    companion object {
        const val maxIterations = 2_000_000
    }
}

fun main(args: Array<String>) {
    val opt: Options = OptionsBuilder()
        .include(SequentialWrites::class.java.simpleName)
        .forks(1)
        .threads(1)
        .measurementIterations(maxIterations)
        .build()
    Runner(opt).run()
}

