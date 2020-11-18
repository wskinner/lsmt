package com.lsmt.table

import ch.qos.logback.classic.Level.DEBUG
import com.lsmt.cache.TableCache
import com.lsmt.core.*
import com.lsmt.domain.Entry
import com.lsmt.domain.StandardLevel
import com.lsmt.log.*
import com.lsmt.manifest.*
import com.lsmt.toKey
import com.lsmt.treeFactory
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.mockk
import java.util.*

class SSTableManagerSpec : StringSpec({
    "write many tables" {
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, DefaultConfig.sstablePrefix)
        val fileManager = createSSTableManager(sstableFileGenerator)

        val tables = mutableListOf<SSTableMetadata>()
        fileManager.use {
            for ((key, value) in entrySeq()) {
                fileManager.append(key, value)
                if (fileManager.totalBytes() > DefaultConfig.maxSstableSize) {
                    val handle = fileManager.rotate()
                    tables.add(
                        SSTableMetadata(
                            sstableFileGenerator.path(handle.id).toString(),
                            "".toByteArray().toKey(),
                            "".toByteArray().toKey(),
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
                        "".toByteArray().toKey(),
                        "".toByteArray().toKey(),
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

    "merge tables" {
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, DefaultConfig.sstablePrefix)
        val walFileGenerator = mockk<SynchronizedFileGenerator>()
        val fileManager = createSSTableManager(sstableFileGenerator)

        val tables = mutableListOf<SSTableMetadata>()
        var totalEntries = 0
        fileManager.use {
            for ((key, value) in entrySeq()) {
                totalEntries++
                fileManager.append(key, value)
                if (fileManager.totalBytes() > DefaultConfig.maxSstableSize) {
                    val handle = fileManager.rotate()
                    tables.add(
                        SSTableMetadata(
                            sstableFileGenerator.path(handle.id).toString(),
                            "".toByteArray().toKey(),
                            "".toByteArray().toKey(),
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
                        "".toByteArray().toKey(),
                        "".toByteArray().toKey(),
                        0,
                        handle.id,
                        handle.totalBytes
                    )
                )
            }
        }

        val tableSum = tables
            .map { BinarySSTableReader().readAll(it).size }
            .sum()

        tableSum shouldBe totalEntries

        val cache = TableCache(
            BinarySSTableReader(),
            DefaultConfig.maxCacheSizeMB,
            sstableFileGenerator = sstableFileGenerator,
            walFileGenerator = walFileGenerator
        )

        val mergedEntries = merge(tables, cache).toMap()
        mergedEntries.size shouldBe totalEntries
        for ((key, value) in entrySeq()) {
            mergedEntries[key] shouldBe value
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
        (manifest.level(1).size()) shouldBe 5

        for (entry in entries) {
            tree.get(entry.first) shouldBe entry.second
        }
    }

    "merge level 0 to level 1 shuffled" {
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
        val entries = fillTree(tree, shuffled = true)
        tree.close()
        (manifest.level(0).size()) shouldBe 3
        (manifest.level(1).size()) shouldBe 11

        for (entry in entries) {
            tree.get(entry.first) shouldBe entry.second
        }
    }

    "no exception" {
        val random = Random(0)

        val tree = treeFactory()
        for (key in longBytesSeq().take(100)) {
            val value = ByteArray(random.nextInt(1000) + 5).apply {
                random.nextBytes(this)
            }
            tree.put(
                key.toKey(), value
            )
        }
        tree.close()
    }
})

fun tree(manifestManager: ManifestManager): LogStructuredMergeTree {
    val walDir = createTempDir().apply { deleteOnExit() }
    val sstableDir = createTempDir().apply { deleteOnExit() }
    val walFileGenerator = SynchronizedFileGenerator(walDir, DefaultConfig.walPrefix)
    val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, DefaultConfig.sstablePrefix)

    val tableController = StandardSSTableController(
        DefaultConfig.maxSstableSize,
        manifestManager,
        TableCache(
            BinarySSTableReader(),
            DefaultConfig.maxCacheSizeMB,
            sstableFileGenerator = sstableFileGenerator,
            walFileGenerator = walFileGenerator
        ),
        sstableFileGenerator,
        AdaptiveCompactionStrategy()
    )

    val tableManager = StandardSSTableManager(
        sstableDir,
        manifestManager,
        DefaultConfig,
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
        DefaultConfig,
        DEBUG
    ).apply { start() }
}

fun entrySeq() = sequence<Entry> {
    val random = Random(0)
    for (key in longBytesSeq()
        .map { it.toKey() }
        .take(200_000)) {
        val value = ByteArray(random.nextInt(200) + 25).apply {
            random.nextBytes(this)
        }

        yield(key to value)
    }
}

fun fillTree(tree: LogStructuredMergeTree, shuffled: Boolean = false): List<Entry> {
    val entries = mutableListOf<Entry>()
    val seq = if (shuffled)
        entrySeq().shuffled()
    else
        entrySeq()

    for ((key, value) in seq) {
        tree.put(key, value!!)
        entries.add(key to value)
    }

    return entries
}
