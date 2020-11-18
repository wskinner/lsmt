package com.lsmt

import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.StandardLevel
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.log.*
import com.lsmt.table.*
import java.util.*

/**
 * Leaving this here until I figure out how to get profiling to work with JMH + Gradle.
 */

fun parseConfig(): Config {
    return Config
}

const val keyRangeSize = 10_000_000

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
        val value = ByteArray(100).apply {
            random.nextBytes(this)
        }

        put(key, value)
    }
}

fun main() {
    val tree = treeFactory()
    tree.fillTree(keySeq())
    tree.close()
}

fun treeFactory(): StandardLogStructuredMergeTree {
    val manifestFile = createTempFile("manifest").apply { deleteOnExit() }
    val sstableDir = createTempDir("sstable").apply { deleteOnExit() }
    val walDir = createTempDir("wal").apply { deleteOnExit() }

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

    val tableController = StandardSSTableController(
        config.maxSstableSize,
        manifestManager,
        TableCache(
            BinarySSTableReader(),
            config.maxCacheSizeMB,
            sstableFileGenerator = sstableFileGenerator,
            walFileGenerator = walFileGenerator
        ),
        fileGenerator = sstableFileGenerator,
        mergeStrategy = AdaptiveCompactionStrategy()
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
            config,
            tableController
        ),
        createWalManager(walFileGenerator),
        config
    ).apply { start() }
}
