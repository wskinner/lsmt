package com.lsmt.table

import com.lsmt.core.Record
import com.lsmt.core.entries
import com.lsmt.log.BinaryWriteAheadLogManager
import com.lsmt.log.FileGenerator
import com.lsmt.log.createLogReader
import com.lsmt.merge
import mu.KotlinLogging
import java.nio.file.Path

interface SSTableController {

    /**
     * Merge one or more tables from level i and 0 or more tables from level i + 1 into level i + 1.
     */
    fun merge(level: Int)

    /**
     * Create a new table file from the log and return the metadata. The caller is responsible for updating the index.
     */
    fun addTableFromLog(logPath: Path): SSTableMetadata
}

class StandardSSTableController(
    private val maxTableSize: Int,
    private val manifest: ManifestManager,
    private val fileGenerator: FileGenerator
) : SSTableController {

    /**
     * Do an N-way merge of all the entries from all N tables. The tables are already in sorted order, but it's possible
     * that some keys are present in both the older and the younger level. In that case, we must drop the older values
     * in favor of the younger ones which supersede them. We must also handle deletions. We need to retain deletion
     * tombstones until we can guarantee that no higher levels in the tree contain leftover entries for the deleted key.
     */
    override fun merge(level: Int) = synchronized(manifest) {
        val start = System.nanoTime()

        val targetLevel = level + 1

        val sourceTables = if (level == 0) {
            manifest.level(level).asList()
        } else {
            val nextCompactionCandidate = manifest.level(level).nextCompactionCandidate()
            if (nextCompactionCandidate != null)
                listOf(nextCompactionCandidate)
            else
                emptyList()
        }
        val destinationTables = sourceTables
            .flatMap { manifest.level(targetLevel).getRange(it.keyRange) }

        val ids = sourceTables.map { it.id }

        val fileManager = BinaryWriteAheadLogManager(fileGenerator)

        logMergeTask(ids, targetLevel, "started")
        try {
            val seq = entries(destinationTables + sourceTables)
                .merge()

            var totalBytes = 0
            var minKey: String? = null
            var maxKey: String? = null

            fun addEntry(key: String, value: Record) {
                maxKey = key

                if (minKey == null)
                    minKey = key

                totalBytes += fileManager.append(key, value)
                if (totalBytes >= maxTableSize) {
                    manifest.addTable(
                        SSTableMetadata(
                            path = fileManager.filePath.toString(),
                            minKey = minKey!!,
                            maxKey = key,
                            level = targetLevel,
                            id = fileManager.id,
                            fileSize = totalBytes
                        )
                    )
                    totalBytes = 0
                    fileManager.rotate()
                    minKey = null
                }
            }

            for (entry in seq) {
                addEntry(entry.key, entry.value)
            }

            for (table in sourceTables) {
                manifest.removeTable(table)
            }

            for (table in destinationTables) {
                manifest.removeTable(table)
            }

            fileManager.close()
            if (totalBytes > 0) {
                manifest.addTable(
                    SSTableMetadata(
                        path = fileManager.filePath.toString(),
                        minKey = minKey!!,
                        maxKey = maxKey!!,
                        level = targetLevel,
                        id = fileManager.id,
                        fileSize = totalBytes
                    )
                )
            }

            logMergeTask(ids, targetLevel, "complete", System.nanoTime() - start)
        } catch (t: Throwable) {
            logMergeTask(ids, targetLevel, "failed", System.nanoTime() - start, t)
            fileManager.close()
        }
    }

    private fun logMergeTask(
        ids: List<Int>,
        targetLevel: Int,
        status: String,
        durationNanos: Long? = null,
        t: Throwable? = null
    ) {
        val baseMessage = "MergeTask=$ids targetLevel=$targetLevel status=$status"
        val message = if (durationNanos != null) {
            val durationMillis = durationNanos / 1000000
            val durationSeconds = "%.2f".format(durationMillis / 1000.0)
            "$baseMessage duration=$durationSeconds"
        } else {
            baseMessage
        }

        if (t != null) {
            logger.error(t) { message }
        } else {
            logger.info { message }
        }
    }

    override fun addTableFromLog(logPath: Path): SSTableMetadata {
        // Read the wal file, merge and sort its contents, and write the result to the new table file.

        BinaryWriteAheadLogManager(fileGenerator).use { writer ->
            val data = try {
                val data = createLogReader(logPath).read().merge()
                data
            } catch (e: Throwable) {
                throw e
            }

            var totalBytes = 0
            data.forEach { entry ->
                totalBytes += writer.append(entry.key, entry.value)
            }

            return SSTableMetadata(
                path = writer.filePath.toString(),
                minKey = data.firstKey(),
                maxKey = data.lastKey(),
                level = 0,
                id = writer.id,
                fileSize = totalBytes
            )
        }
    }


    companion object {
        val logger = KotlinLogging.logger { }
    }
}


