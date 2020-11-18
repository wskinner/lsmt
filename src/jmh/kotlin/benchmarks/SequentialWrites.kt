package benchmarks

import Config
import log.StandardCompactor
import benchmarks.SequentialWrites.Companion.maxIterations
import ch.qos.logback.classic.Level.ERROR
import core.LogStructuredMergeTree
import core.StandardLevel
import core.StandardLogStructuredMergeTree
import core.maxLevelSize
import log.BinaryWriteAheadLogManager
import log.BinaryWriteAheadLogWriter
import log.SynchronizedFileGenerator
import log.createLogReader
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Mode.Throughput
import org.openjdk.jmh.runner.Runner
import org.openjdk.jmh.runner.options.Options
import org.openjdk.jmh.runner.options.OptionsBuilder
import parseConfig
import table.*
import java.util.*
import java.util.concurrent.TimeUnit


/**
 * This benchmark attempts to measure the write throughput with sequential keys.
 */
@State(Scope.Thread)
@BenchmarkMode(Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
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

    @OutputTimeUnit(TimeUnit.SECONDS)
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

fun treeFactory(): StandardLogStructuredMergeTree {
    val manifestFile = createTempFile("manifest").apply { deleteOnExit() }
    val sstableDir = createTempDir("sstable").apply { deleteOnExit() }
    val walDir = createTempDir("wal").apply { deleteOnExit() }

    val config = parseConfig()

    val manifestManager = StandardManifestManager(
        BinaryManifestWriter(
            BinaryWriteAheadLogWriter(manifestFile.outputStream())
        ),
        BinaryManifestReader(
            createLogReader(manifestFile.toPath())
        ),
        levelFactory = { StandardLevel() }
    )

    val tableController = StandardSSTableController(
        sstableDir,
        Config.sstablePrefix,
        Config.maxSstableSize,
        manifestManager,
        SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        StandardSSTableManager(
            sstableDir,
            manifestManager,
            BinarySSTableReader(
                sstableDir,
                Config.sstablePrefix
            ),
            config,
            tableController,
            StandardCompactor(
                manifestManager,
                { maxLevelSize(it) },
                tableController
            )
        ),
        BinaryWriteAheadLogManager(
            walDir,
            Config.walPrefix,
            SynchronizedFileGenerator(walDir, Config.walPrefix)
        ),
        Config,
        ERROR
    ).apply { start() }
}
