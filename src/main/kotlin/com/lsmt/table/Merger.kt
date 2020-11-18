package com.lsmt.table

import com.lsmt.cache.TableCache
import com.lsmt.domain.Entry
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.domain.TableEntry
import com.lsmt.log.FileGenerator
import com.lsmt.log.createSSTableManager
import com.lsmt.manifest.ManifestManager
import com.lsmt.manifest.SSTableMetadata
import mu.KotlinLogging
import java.util.*

class Merger(
    private val tableCache: TableCache,
    private val fileGenerator: FileGenerator,
    private val manifest: ManifestManager,
    private val mergeStrategy: MergeStrategy,
    private val maxTableSize: Int,
    private val level: Int,
    private val compactionId: Long
) : AutoCloseable {

    private val fileManager = createSSTableManager(fileGenerator)

    private val targetLevel = level + 1

    var minKey: Key? = null

    var maxKey: Key? = null

    fun merge() {
        val start = System.nanoTime()
        val mergeTask = mergeStrategy.mergeTargets(level, manifest)
        val targetLevel = level + 1

        val destTables = mutableSetOf<SSTableMetadata>()
        val sourceTables = mutableListOf<SSTableMetadata>()
        for (table in mergeTask.sourceTables) {
            val overlap = manifest.level(targetLevel).getRange(table.keyRange)
            if (overlap.isEmpty()) {
                moveTableUp(table)
            } else {
                sourceTables.add(table)
                destTables.addAll(overlap)
            }
        }

        if (destTables.isEmpty()) {
            StandardSSTableController.logger.debug("No destination tables. Returning.")
            return
        }

        val ids = sourceTables.map { it.id }
        logMergeTask(ids, targetLevel, compactionId, "started")

        try {
            val sorted = merge(sourceTables + destTables, tableCache)

            for (entry in sorted) {
                addEntry(entry.first, entry.second)
            }

            for (table in sourceTables) {
                manifest.removeTable(table)
            }

            for (table in destTables) {
                manifest.removeTable(table)
            }

            logMergeTask(ids, targetLevel, compactionId, "complete", System.nanoTime() - start)
            logger.info { manifest.toString() }
        } catch (t: Throwable) {
            logMergeTask(ids, targetLevel, compactionId, "failed", System.nanoTime() - start, t)
            fileManager.close()
        }
    }

    private fun addEntry(key: Key, value: Record?) {
        maxKey = key

        if (minKey == null)
            minKey = key

        tableCache.write(fileManager.id, key, value)
        fileManager.append(key, value)

        if (fileManager.totalBytes() >= maxTableSize) {
            newTable()
        }
    }

    private fun newTable() {
        val logHandle = fileManager.rotate()
        manifest.addTable(
            SSTableMetadata(
                path = fileGenerator.path(logHandle.id).toString(),
                minKey = minKey!!,
                maxKey = maxKey!!,
                level = targetLevel,
                id = logHandle.id,
                fileSize = logHandle.totalBytes
            )
        )
        minKey = null
        maxKey = null
    }

    private fun logMergeTask(
        ids: List<Long>,
        targetLevel: Int,
        compactionId: Long,
        status: String,
        durationNanos: Long? = null,
        t: Throwable? = null
    ) {
        val baseMessage = "MergeTask id=$compactionId targetLevel=$targetLevel tables=$ids status=$status"
        val message = if (durationNanos != null) {
            val durationMillis = durationNanos / 1000000
            val durationSeconds = "%.2f".format(durationMillis / 1000.0)
            "$baseMessage duration=$durationSeconds"
        } else {
            baseMessage
        }

        if (t != null) {
            StandardSSTableController.logger.error(t) { message }
        } else {
            StandardSSTableController.logger.info { message }
        }
    }

    override fun close() {
        if (maxKey != null) {
            newTable()
        } else {
            fileManager.close()
        }
    }

    /**
     * Move a table from its current level to the next level. We can safely do this when a table is to be compacted, but
     * it does not overlap with any tables in the next level. This avoids the work of deserializing, sorting, and
     * serializing the contents.
     */
    private fun moveTableUp(table: SSTableMetadata) {
        StandardSSTableController.logger.debug("Moving table. table=${table.id} newLevel=${table.level + 1}")
        val newTableMeta = table.copy(level = table.level + 1)
        manifest.removeTable(table)
        manifest.addTable(newTableMeta)
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

/**
 * Merge one or more tables into a sorted stream of Entries. If there are entries with duplicate keys, the key from
 * the younger table wins.
 */
fun merge(tables: Collection<SSTableMetadata>, tableCache: TableCache): Sequence<Entry> = sequence {
    val priorityQueue = PriorityQueue<TableEntry>()
    val iterators = tables
        .map { it.id to tableCache.read(it).iterator() }
        .toMap()
    for ((i, iter) in iterators)
        priorityQueue.add(TableEntry(iter.next(), i))

    var buffer = priorityQueue.poll()
    if (iterators.getValue(buffer.tableId).hasNext())
        priorityQueue.add(TableEntry(iterators.getValue(buffer.tableId).next(), buffer.tableId))

    while (priorityQueue.isNotEmpty()) {
        val next = priorityQueue.poll()
        if (iterators.getValue(next.tableId).hasNext())
            priorityQueue.add(TableEntry(iterators.getValue(next.tableId).next(), next.tableId))

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
