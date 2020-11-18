import core.LogStructuredMergeTree
import core.StandardLevel
import core.StandardLogStructuredMergeTree
import core.maxLevelSize
import log.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import table.*
import java.util.*
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

/**
 * Leaving this here until I figure out how to get profiling to work with JMH + Gradle.
 */

/**
 * Write some key-value pairs. Keys are sequential. Values are on the order of ~10 to ~1000 bytes.
 */
fun sequentialWrites(times: Int, ops: Int, treeFactory: () -> LogStructuredMergeTree) {
    println("Testing sequential writes")
    val runs = ArrayList<Long>()
    val random = Random(0)

    for (run in 1..times) {
        val tree = treeFactory()
        val start = System.nanoTime()
        for (i in 1..ops) {
            val name = ByteArray(random.nextInt(1000) + 5).run {
                random.nextBytes(this)
                Base64.getEncoder().encodeToString(this)!!
            }
            tree.put(
                "person$i", sortedMapOf(
                    "name" to name,
                    "age" to random.nextInt()
                )
            )
        }
        tree.close()
        val end = System.nanoTime()
        runs.add(end - start)
    }

    val opsPerSecond = "%.2f".format((ops * times).toDouble() / (runs.sum() / 1_000_000_000.0))
    println("Ops per second: $opsPerSecond")
    println(runs)
}

/**
 * Write some key-value pairs. Keys are random. Values are on the order of ~10 to ~1000 bytes.
 */
fun randomWrites(times: Int, ops: Int, treeFactory: () -> LogStructuredMergeTree) {
    println("Testing random writes")
    val runs = ArrayList<Long>()
    val random = Random(0)

    for (run in 1..times) {
        val tree = treeFactory()
        val start = System.nanoTime()
        for (i in 1..ops) {
            val key = ByteArray(16).run {
                random.nextBytes(this)
                Base64.getEncoder().encodeToString(this)!!
            }
            val name = ByteArray(random.nextInt(1000) + 5).run {
                random.nextBytes(this)
                Base64.getEncoder().encodeToString(this)!!
            }
            tree.put(
                key, sortedMapOf(
                    "name" to name,
                    "age" to random.nextInt()
                )
            )
        }
        tree.close()
        val end = System.nanoTime()
        runs.add(end - start)
    }

    val opsPerSecond = "%.2f".format((ops * times).toDouble() / (runs.sum() / 1_000_000_000.0))
    println("Ops per second: $opsPerSecond")
    println(runs)
}

fun parseConfig(): Config {
    return Config
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
        config.sstablePrefix,
        config.maxSstableSize,
        manifestManager,
        SynchronizedFileGenerator(sstableDir, config.sstablePrefix)
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
        Config
    ).apply { start() }
}

fun main() {
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
//    rootLogger.level = Level.ERROR
    thread(start = true) {
        sequentialWrites(5, 2_000_000, treeFactory = { treeFactory() })
    }.join()

    thread(start = true) {
        randomWrites(5, 2_000_000, treeFactory = { treeFactory() })
    }.join()
    println("Run complete.")
}
