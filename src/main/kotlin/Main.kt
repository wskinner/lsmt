import core.LogStructuredMergeTree
import core.StandardLevel
import core.StandardLogStructuredMergeTree
import core.maxLevelSize
import log.BinaryWriteAheadLogManager
import log.BinaryWriteAheadLogWriter
import log.createLogReader
import table.*
import java.io.File
import java.util.*
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

fun test(times: Int, ops: Int, tree: LogStructuredMergeTree) {
    val runs = ArrayList<Long>()
    for (run in 1..times) {
        val start = System.nanoTime()
        for (i in 1..ops) {
            tree.put(
                "person$i", sortedMapOf(
                    "name" to "will",
                    "age" to 29
                )
            )
        }
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

fun main() {
    File("./build/manifest").apply { mkdirs() }
    val manifestFile = File("./build/manifest/manifest.log").apply { createNewFile() }
    val sstableDir = File("./build/sstable").apply { mkdirs() }
    val walDir = File("./build/log").apply { mkdirs() }

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
        config.maxSstableSize
    )

    val tree = StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        StandardSSTableManager(
            File("./build/sstables"),
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
            Config.walPrefix
        ),
        Config
    ).apply { start() }

    tree.use {
        thread(start = true) {
            test(5, 100_000, tree)
        }.join()
    }

    println("Run complete.")
}

//    print("Reads")
//    for (i in 1..100000) {
//        tree.get("person$i")
//    }
