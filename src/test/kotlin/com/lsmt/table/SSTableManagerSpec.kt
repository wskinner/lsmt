package com.lsmt.table

import ch.qos.logback.classic.Level.DEBUG
import com.lsmt.Config
import com.lsmt.core.Entry
import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.StandardLevel
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.log.*
import com.lsmt.treeFactory
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.util.*

class SSTableManagerSpec : StringSpec({
    "write many tables" {
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)
        val fileManager = createSSTableManager(sstableFileGenerator)

        val tables = mutableListOf<SSTableMetadata>()
        fileManager.use {
            for ((key, value) in entrySeq()) {
                fileManager.append(key, value)
                if (fileManager.totalBytes() > Config.maxSstableSize) {
                    val handle = fileManager.rotate()
                    tables.add(
                        SSTableMetadata(
                            sstableFileGenerator.path(handle.id).toString(),
                            "",
                            "",
                            0,
                            handle.id,
                            handle.totalBytes
                        )
                    )
                }
            }
            if (fileManager.totalBytes() > 0) {
                val handle = fileManager.rotate()
                tables.add(
                    SSTableMetadata(
                        sstableFileGenerator.path(handle.id).toString(),
                        "",
                        "",
                        0,
                        handle.id,
                        handle.totalBytes
                    )
                )
            }
        }

        val reader = BinarySSTableReader()
        for (table in tables) {
            reader.readAll(table).toList()
            val iter = reader.mmap(table).iterator()
            iter.asSequence().toList()
        }
    }

    "merge level 0 to level 1" {
        val manifestDir = createTempDir().apply { deleteOnExit() }
        val manifestFile = createTempFile(directory = manifestDir).apply { deleteOnExit() }
        val manifest = StandardManifestManager(
            BinaryManifestWriter(
                BinaryLogWriter(manifestFile.outputStream())
            ),
            BinaryManifestReader(
                createLogReader(manifestFile.toPath())
            ),
            levelFactory = { StandardLevel(it) }
        )
        val tree = tree(manifest)
        val entries = fillTree(tree)
        tree.close()
        (manifest.level(0).size()) shouldBe 3
        (manifest.level(1).size()) shouldBe 10

        for (entry in entries) {
            tree.get(entry.first) shouldBe entry.second
        }
    }

    "no exception" {
        val random = Random(0)

        val tree = treeFactory()
        for (i in 1..100) {
            val value = ByteArray(random.nextInt(1000) + 5).apply {
                random.nextBytes(this)
            }
            tree.put(
                "person$i", value
            )
        }
        tree.close()
    }
})

fun tree(manifestManager: ManifestManager): LogStructuredMergeTree {
    val walDir = createTempDir().apply { deleteOnExit() }
    val sstableDir = createTempDir().apply { deleteOnExit() }
    val walFileGenerator = SynchronizedFileGenerator(walDir, Config.walPrefix)
    val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)

    val tableController = StandardSSTableController(
        Config.maxSstableSize,
        manifestManager,
        TableCache(
            BinarySSTableReader(),
            Config.maxCacheSizeMB,
            sstableFileGenerator = sstableFileGenerator,
            walFileGenerator = walFileGenerator
        ),
        sstableFileGenerator,
        AdaptiveCompactionStrategy()
    )

    val tableManager = StandardSSTableManager(
        sstableDir,
        manifestManager,
        Config,
        tableController
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        tableManager,
        createWalManager(walFileGenerator),
        Config,
        DEBUG
    ).apply { start() }
}

fun entrySeq() = sequence<Entry> {
    val random = Random(0)
    for (i in 0..200_000) {
        val key = "key$i"
        val value = ByteArray(random.nextInt(200) + 25).apply {
            random.nextBytes(this)
        }

        yield(key to value)
    }
}

fun fillTree(tree: LogStructuredMergeTree): List<Entry> {
    val entries = mutableListOf<Entry>()

    for ((key, value) in entrySeq()) {
        tree.put(key, value!!)
        entries.add(key to value)
    }

    return entries
}
