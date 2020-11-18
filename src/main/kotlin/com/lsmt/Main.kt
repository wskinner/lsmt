package com.lsmt

import com.lsmt.cache.TableCache
import com.lsmt.core.*
import com.lsmt.domain.Key
import com.lsmt.domain.StandardLevel
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.SynchronizedFileGenerator
import com.lsmt.log.createLogReader
import com.lsmt.log.createWalManager
import com.lsmt.manifest.BinaryManifestReader
import com.lsmt.manifest.BinaryManifestWriter
import com.lsmt.manifest.StandardManifestManager
import com.lsmt.table.*
import java.util.*

/**
 * Leaving this here until I figure out how to get profiling to work with JMH + Gradle.
 */

fun parseConfig(): Config {
    return DefaultConfig
}

const val keyRangeSize = 10_000_000

fun LogStructuredMergeTree.fillTree(keySeq: Sequence<Key>) {
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
    tree.fillTree(longBytesSeq().map { it.toKey() })
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
