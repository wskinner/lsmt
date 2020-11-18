package com.lsmt.table

import com.lsmt.Config
import com.lsmt.core.Record
import mu.KotlinLogging
import java.io.File
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Responsible for creating and deleting SSTable files and performing compactions.
 */
interface SSTableManager : AutoCloseable {

    /**
     * Attempt to read a record from the tree. Without optimization, in the worst case, this will scan the metadata of
     * every table.
     */
    fun get(key: String): Record?

    /**
     * Create a new table from the log file.
     */
    fun addTableFromLog(log: MemTable)

    /**
     * Create a new table from the log file.
     */
    fun addTableFromLog(logId: Long)

    /**
     * Asynchronously create a new table from the log file.
     */
    fun addTableAsync(log: MemTable, id: Long)
}

/**
 * SSTable files are arranged in levels. Tables in level zero, the "young" level can have overlapping keys. Tables in
 * other levels must contain no overlapping keys. I.e. for levels greater than zero, a key will be within the key range
 * of at most one table. Since records gradually move from the lower to the higher levels, this implies that for a given
 * key, the record in the lowest level must be the most recent (except for the records in the young level, where overlap
 * may occur).
 */
class StandardSSTableManager(
    // Directory where the SSTable files will be stored
    rootDirectory: File,
    private val manifest: ManifestManager,
    private val config: Config,
    private val tableController: SSTableController
) : SSTableManager {

    // This pool is responsible for making new SSTable files from log files. This task is parallelizable with no
    // contention. Because it is IO bound, it is safe to create many threads here.
    private val tableCreationPool: ExecutorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
        ) { Thread(it, nextThreadName("sstable")) }

    private val activeTableCreationTasks = AtomicInteger()

    init {
        if (!rootDirectory.exists()) {
            rootDirectory.mkdir()
        }
    }

    override fun get(key: String): Record? = getYoung(key) ?: getOld(key)

    /**
     * When the size of the young level exceeds a threshold, merge all young level files into all overlapping files in
     * level 1.
     */
    override fun addTableFromLog(log: MemTable) {
        activeTableCreationTasks.incrementAndGet()
        manifest.addTable(tableController.addTableFromLog(log))

        if (manifest.level(0).size() > config.maxYoungTables) {
            tableController.addCompactionTask(0)
        }
        activeTableCreationTasks.decrementAndGet()
    }

    override fun addTableFromLog(logId: Long) {
        manifest.addTable(tableController.addTableFromLog(logId))

        if (manifest.level(0).size() > config.maxYoungTables) {
            tableController.addCompactionTask(0)
        }
    }

    /**
     * Switch on the current number of active compactions. If it's too high, drop the memtable and add a task
     * which will read the log from disk when it eventually runs. This should reduce memory pressure.
     */
    override fun addTableAsync(log: MemTable, id: Long) {
        if (activeTableCreationTasks.get() > config.maxActiveTableCreationTasks) {
            tableCreationPool.submit {
                addTableFromLog(id)
            }
        } else {
            tableCreationPool.submit {
                addTableFromLog(log)
            }
        }
    }

    override fun close() {
        logger.info("Shutting down SSTableManager")
        logger.info("Active log copy tasks: ${activeTableCreationTasks.get()}")

        logger.info("Awaiting thread pool termination")
        tableCreationPool.shutdown()
        tableCreationPool.awaitTermination(1, TimeUnit.MINUTES)
        logger.info("Thread pool termination complete.")
        logger.info("Active log copy tasks: ${activeTableCreationTasks.get()}")

        logger.info("Shutting down table controller")
        tableController.close()
        logger.info("table controller termination complete")

        manifest.close()
        logger.info("Done shutting down SSTableManager")
    }

    override fun toString(): String = manifest.toString()

    /**
     * Search the young level (level 0) for a key. Tables in the young level may have overlapping keys, so we need to
     * scan all the tables that might contain the most recent record.
     *
     */
    private fun getYoung(key: String): Record? =
        manifest.level(0)
            .get(key)
            .map { it.id to tableController.read(it, key) }
            .maxBy { it.first }
            ?.second

    /**
     * Search all the levels except level 0 for a key. The search proceeds top down, so the newest occurrence of the key
     * will be returned, if the key exists in the database. This should be optimized later by adding a Bloom Filter per
     * level, or something similar.
     *
     * The true performance of this function depends on the implementation of the level structure. This function runs in
     * O(L * Q) where L is the number of levels and Q is the time to query a level. If the level structure is
     * efficiently implemented using an interval tree, this will come to O(L * log N) where N is the number of tables.
     * If the level structure is implemented naively, this will come to O(N).
     */
    private fun getOld(key: String): Record? {
        for (level in manifest.levels()) {
            if (level.key != 0) {
                // For levels except the young level, there must be at most one table whose range includes each key.
                val table = level.value.get(key).firstOrNull()
                if (table != null) {
                    return tableController.read(table, key)
                }
            }
        }
        return null
    }

    companion object {
        private val logger = KotlinLogging.logger {}

        private val nextThread = mutableMapOf<String, AtomicInteger>()

        fun nextThreadName(prefix: String): String {
            nextThread.putIfAbsent(prefix, AtomicInteger())
            return "$prefix-${nextThread.getValue(prefix).getAndIncrement()}"
        }
    }

}
