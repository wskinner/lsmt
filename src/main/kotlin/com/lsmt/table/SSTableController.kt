package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.core.TableEntry
import com.lsmt.core.maxLevelSize
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.FileGenerator
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

interface SSTableController : AutoCloseable {

    /**
     * Merge one or more tables from level i and 0 or more tables from level i + 1 into level i + 1.
     */
    fun merge(level: Int)

    /**
     * Create a new table file from the log and return the metadata. The caller is responsible for updating the index.
     */
    fun addTableFromLog(id: Int): SSTableMetadata

    fun addCompactionTask(level: Int)
}

class StandardSSTableController(
    private val maxTableSize: Int,
    private val manifest: ManifestManager,
    private val tableCache: TableCache,
    private val fileGenerator: FileGenerator
) : SSTableController {

    // This pool handles compaction of tables. This task is inherently contentious. In theory, we can lock at the level
    // of the individual table. I'm not yet sure of the best way to handle this.
    private val compactionPool = Executors.newSingleThreadExecutor {
        Thread(
            it,
            StandardSSTableManager.nextThreadName("compaction")
        )
    }

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

        // Tables that don't overlap with any table in the destination level can be moved to that level.
        val destinationTables = mutableListOf<SSTableMetadata>()
        sourceTables.forEach {
            val overlap = manifest.level(targetLevel).getRange(it.keyRange)
            if (overlap.isEmpty()) {
                moveTableUp(it)
            } else {
                destinationTables.addAll(overlap)
            }
        }

        val ids = sourceTables.map { it.id }

        val fileManager = BinaryLogManager(fileGenerator)
        logMergeTask(ids, targetLevel, "started")
        try {
            val seq = merge(destinationTables + sourceTables)

            var totalBytes = 0
            var minKey: String? = null
            var maxKey: String? = null

            fun addEntry(key: String, value: Record?) {
                maxKey = key

                if (minKey == null)
                    minKey = key

                totalBytes += fileManager.append(key, value)
                tableCache.write(fileManager.id, key, value)
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
                addEntry(entry.first, entry.second)
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
        } finally {
            if (manifest.level(level + 1).size() > maxLevelSize(level + 1)) {
                addCompactionTask(level + 1)
            }
            addCleanupTask(level)
        }
    }

    /**
     * Move a table from its current level to the next level. We can safely do this when a table is to be compacted, but
     * it does not overlap with any tables in the next level. This avoids the work of deserializing, sorting, and
     * serializing the contents.
     */
    private fun moveTableUp(table: SSTableMetadata) {
        logger.debug("Moving table. table=${table.id} newLevel=${table.level + 1}")
        val newTableMeta = table.copy(level = table.level + 1)
        manifest.removeTable(table)
        manifest.addTable(newTableMeta)
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

    override fun addTableFromLog(id: Int): SSTableMetadata = tableCache.write(id)

    override fun addCompactionTask(level: Int) {
        logger.debug("Adding compaction task for level=$level")
        compactionPool.submit { merge(level) }
    }

    override fun close() {
        logger.info("Shutting down compaction pool")
        compactionPool.shutdown()
        logger.info("Awaiting compaction pool termination")
        compactionPool.awaitTermination(5L, TimeUnit.MINUTES)
        logger.info("Compaction pool termination complete")
    }

    private fun addCleanupTask(level: Int) {
        compactionPool.submit { removeUnusedTables(level) }
    }

    private fun removeUnusedTables(level: Int) {
        logger.debug("removeUnusedTables() is not yet implemented")
    }

    /**
     * Merge one or more tables into a sorted stream of Entries. If there are entries with duplicate keys, the key from
     * the younger table wins.
     */
    private fun merge(tables: List<SSTableMetadata>): Sequence<Entry> = merge(tables, tableCache)

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

/**
 * Merge one or more tables into a sorted stream of Entries. If there are entries with duplicate keys, the key from
 * the younger table wins.
 */
fun merge(tables: List<SSTableMetadata>, tableCache: TableCache): Sequence<Entry> = sequence {
    val priorityQueue = PriorityQueue<TableEntry>()
    val iterators = tables.sortedBy { it.id }
        .map { tableCache.read(it).iterator() }
        .toTypedArray()
    for (i in iterators.indices)
        priorityQueue.add(TableEntry(iterators[i].next(), i))

    var buffer = priorityQueue.poll()
    while (priorityQueue.isNotEmpty()) {
        if (iterators[buffer.tableId].hasNext()) {
            priorityQueue.add(TableEntry(iterators[buffer.tableId].next(), buffer.tableId))
        }

        val next = priorityQueue.poll()
        buffer = if (next.entry.first == buffer.entry.first) {
            // An entry from a younger table has the same key. This is either an overwrite or a deletion.
            next
        } else {
            // No entry from a younger table has the same key, so it's safe to yield.
            yield(buffer.entry)
            next
        }
    }
    if (buffer != null)
        yield(buffer.entry)
}
