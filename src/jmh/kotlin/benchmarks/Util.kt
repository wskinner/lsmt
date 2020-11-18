package benchmarks

import ch.qos.logback.classic.Level
import com.lsmt.cache.TableCache
import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.core.StandardMemTable
import com.lsmt.core.longBytesSeq
import com.lsmt.domain.StandardLevel
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.SynchronizedFileGenerator
import com.lsmt.log.createLogReader
import com.lsmt.log.createWalManager
import com.lsmt.manifest.BinaryManifestReader
import com.lsmt.manifest.BinaryManifestWriter
import com.lsmt.manifest.StandardManifestManager
import com.lsmt.parseConfig
import com.lsmt.table.*
import com.lsmt.toKey
import java.util.*

fun treeFactory(): StandardLogStructuredMergeTree {
    val manifestFile = createTempFile("manifest").apply { deleteOnExit() }
    val sstableDir = createTempDir("sstable")
    val walDir = createTempDir("wal")

    val config = parseConfig()

    val manifestManager = StandardManifestManager(
        BinaryManifestWriter(
            BinaryLogWriter(manifestFile.outputStream())
        ),
        BinaryManifestReader(
            createLogReader(manifestFile.toPath())
        ),
        levelFactory = { StandardLevel(it) }
    )

    val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, config.sstablePrefix)
    val walFileGenerator = SynchronizedFileGenerator(walDir, config.walPrefix)
    val tableCache = TableCache(
        BinarySSTableReader(),
        config.maxCacheSizeMB,
        sstableFileGenerator = sstableFileGenerator,
        walFileGenerator = walFileGenerator
    )

    val tableController = StandardSSTableController(
        config.maxSstableSize,
        manifestManager,
        tableCache,
        sstableFileGenerator,
        AdaptiveCompactionStrategy()
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Deleting all sstable and wal files")
        val sstableResult = sstableDir.deleteRecursively()
        val walResult = walDir.deleteRecursively()
        if (sstableResult)
            println("sstables deleted")
        else
            println("sstables not deleted")

        if (walResult)
            println("wals deleted")
        else
            println("wals not deleted")
    })

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        StandardSSTableManager(
            sstableDir,
            manifestManager,
            config,
            tableController
        ),
        createWalManager(walFileGenerator),
        config,
        Level.INFO
    ).apply { start() }
}


fun keySeq() = longBytesSeq().map { it.toKey() }

fun entrySeq(keyRangeSize: Int, valueSize: Int) = sequence {
    val random = Random(0)
    for (key in keySeq().take(keyRangeSize)) {
        val value = ByteArray(valueSize).apply {
            random.nextBytes(this)
        }
        yield(key to value)
    }
}

fun LogStructuredMergeTree.fillTree(keyRangeSize: Int, valueSize: Int = 100) {
    for (entry in entrySeq(keyRangeSize, valueSize))
        put(entry.first, entry.second)
}

fun <T> Sequence<T>.repeat(n: Int) = sequence { repeat(n) { yieldAll(this@repeat) } }
