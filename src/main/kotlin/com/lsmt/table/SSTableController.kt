package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.core.TableEntry
import com.lsmt.core.maxLevelSize
import com.lsmt.log.FileGenerator
import com.lsmt.log.createSSTableManager
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

interface SSTableController : AutoCloseable {

    /**
     * Merge one or more tables from level i and 0 or more tables from level i + 1 into level i + 1.
     */
    fun merge(level: Int)

    fun read(table: SSTableMetadata, key: String): Record?

    /**
     * Create a new table file from the log and return the metadata. The caller is responsible for updating the index.
     */
    fun addTableFromLog(id: Long): SSTableMetadata

    fun addTableFromLog(log: MemTable): SSTableMetadata

    fun addCompactionTask(level: Int)
}

class StandardSSTableController(
    private val maxTableSize: Int,
    private val manifest: ManifestManager,
    private val tableCache: TableCache,
    private val fileGenerator: FileGenerator,
    private val mergeStrategy: MergeStrategy
) : SSTableController {
    private val compactionsPerformed = AtomicLong()

    private val compactionsQueued = AtomicLong()

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
        val compactionId = compactionsPerformed.getAndIncrement()
        val compactionsRemaining = compactionsQueued.decrementAndGet()
        logger.info { "id=$compactionId compactionsRemaining=$compactionsRemaining" }

        for (i in (level) until manifest.levels().size) {
            if (manifest.level(i).size() > maxLevelSize(i)) {
                logger.debug { "id=$compactionId compacting level=$i" }
                doMerge(i, compactionId)
            } else {
                logger.debug { "id=$compactionId skipping level=$i" }
            }
        }
    }

    override fun read(table: SSTableMetadata, key: String): Record? = tableCache.read(table, key)

    private fun doMerge(level: Int, compactionId: Long) {
        val start = System.nanoTime()
        val mergeTask = mergeStrategy.mergeTargets(level, manifest)
        val targetLevel = level + 1

        val destTables = mutableSetOf<SSTableMetadata>()
        mergeTask.sourceTables.forEach {
            val overlap = manifest.level(targetLevel).getRange(it.keyRange)
            if (overlap.isEmpty()) {
                // Tables that don't overlap with any table in the destination level can be moved to that level.
                moveTableUp(it)
            } else {
                destTables.addAll(overlap)
            }
        }

        if (destTables.isEmpty()) {
            logger.debug("No destination tables. Returning.")
            return
        }

        val ids = mergeTask.sourceTables.map { it.id }
        logMergeTask(ids, targetLevel, compactionId, "started")

        val fileManager = try {
            createSSTableManager(fileGenerator)
        } catch (t: Throwable) {
            logger.error(t) { "Couldn't create file manager" }
            return
        }
        try {
            val seq = merge(destTables.toList() + mergeTask.sourceTables)

            var minKey: String? = null
            var maxKey: String? = null

            fun addEntry(key: String, value: Record?) {
                maxKey = key

                if (minKey == null)
                    minKey = key

                tableCache.write(fileManager.id, key, value)
                fileManager.append(key, value)
                if (fileManager.totalBytes() >= maxTableSize) {
                    val logHandle = fileManager.rotate()
                    manifest.addTable(
                        SSTableMetadata(
                            path = fileGenerator.path(logHandle.id).toString(),
                            minKey = minKey!!,
                            maxKey = key,
                            level = targetLevel,
                            id = logHandle.id,
                            fileSize = logHandle.totalBytes
                        )
                    )
                    minKey = null
                }
            }

            for (entry in seq) {
                addEntry(entry.first, entry.second)
            }

            for (table in mergeTask.sourceTables) {
                manifest.removeTable(table)
            }

            for (table in destTables) {
                manifest.removeTable(table)
            }

            val logHandle = fileManager.rotate()
            if (logHandle.totalBytes > 0) {
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
            }

            logMergeTask(ids, targetLevel, compactionId, "complete", System.nanoTime() - start)
            logger.info { manifest.toString() }
        } catch (t: Throwable) {
            logMergeTask(ids, targetLevel, compactionId, "failed", System.nanoTime() - start, t)
            fileManager.close()
        } finally {
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
            logger.error(t) { message }
        } else {
            logger.info { message }
        }
    }

    override fun addTableFromLog(id: Long): SSTableMetadata = tableCache.write(id)

    override fun addTableFromLog(log: MemTable): SSTableMetadata = tableCache.write(log)

    override fun addCompactionTask(level: Int) {
        logger.debug("Adding compaction task for level=$level")
        compactionsQueued.incrementAndGet()
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
