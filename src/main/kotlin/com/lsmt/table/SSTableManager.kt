package com.lsmt.table

import com.lsmt.Config
import com.lsmt.core.Record
import com.lsmt.core.makeFile
import com.lsmt.log.createLogReader
import mu.KotlinLogging
import java.io.File
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
    fun addTableFromLog(logId: Int)

    /**
     * Asynchronously create a new table from the log file.
     */
    fun addTableAsync(logId: Int)
}

/**
 * Handles logic for reading SSTables.
 */
interface SSTableReader {
    fun read(table: SSTableMetadata, key: String): Record?

    fun readAll(table: SSTableMetadata): SortedMap<String, Record?>
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
    tableReader: SSTableReader,
    private val config: Config,
    private val tableController: SSTableController
) : SSTableManager {

    private val cachedTableReader = tableReader

    // This pool is responsible for making new SSTable files from log files. This task is parallelizable with no
    // contention. Because it is IO bound, it is safe to create many threads here.
    private val tableCreationPool: ExecutorService =
        Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
        ) { Thread(it, nextThreadName("sstable")) }

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
    override fun addTableFromLog(logId: Int) {
        manifest.addTable(tableController.addTableFromLog(logId))

        if (manifest.level(0).size() > config.maxYoungTables) {
            tableController.addCompactionTask(0)
        }
    }

    override fun addTableAsync(logId: Int) {
        logger.info("Adding task for log=$logId")
        tableCreationPool.submit {
            addTableFromLog(logId)
        }
    }

    override fun close() {
        logger.info("Shutting down SSTableManager")

        logger.info("Awaiting thread pool termination")
        tableCreationPool.shutdown()
        tableCreationPool.awaitTermination(1, TimeUnit.MINUTES)
        logger.info("Thread pool termination complete")

        logger.info("Shutting down table controller")
        tableController.close()
        logger.info("table controller termination complete")

        manifest.close()
        logger.info("Done shutting down SSTableManager")
    }

    /**
     * Search the young level (level 0) for a key. Tables in the young level may have overlapping keys, so we need to
     * scan all the tables that might contain the most recent record.
     */
    private fun getYoung(key: String): Record? =
        manifest.level(0)
            .get(key)
            .map { it.id to cachedTableReader.read(it, key) }
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
        manifest.levels()
            .filterNot { it.key == 0 }
            .values
            .forEach {
                // For levels except the young level, there must be at most one table whose range includes each key.
                val table = it.get(key).firstOrNull()
                if (table != null) {
                    return cachedTableReader.read(table, key)
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

class BinarySSTableReader(
    private val rootDirectory: File,
    private val prefix: String = Config.sstablePrefix
) : SSTableReader {
    override fun read(table: SSTableMetadata, key: String): Record? {
        val file = makeFile(rootDirectory, prefix, table.id)
        val reader = createLogReader(file)

        return reader.readAll()
            .firstOrNull { it.first == key }
            ?.second
    }

    override fun readAll(table: SSTableMetadata): SortedMap<String, Record?> {
        val file = makeFile(rootDirectory, prefix, table.id)
        val reader = createLogReader(file)
        val entries = reader.readAll()
        val result = TreeMap<String, Record?>()
        for (entry in entries) {
            result[entry.first] = entry.second
        }

        return result
    }
}
