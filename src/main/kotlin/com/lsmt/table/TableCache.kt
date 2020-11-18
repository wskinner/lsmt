package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.log.FileGenerator
import com.lsmt.log.createLogReader
import com.lsmt.log.createSSTableManager
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong

class TableCache(
    private val reader: BinarySSTableReader,
    maxSizeMB: Int,
    private val sstableFileGenerator: FileGenerator,
    private val walFileGenerator: FileGenerator
) {

    private val cacheMisses = AtomicLong()
    private val operations = AtomicLong()

    // Estimate the size of a table in memory using the raw disk size.
    // TODO (will) move the cache off heap
    private val cache = LRUCache<Long, SSTable>(maxSizeMB / 2)

    private val tableWriteSemaphore = Semaphore(10, true)

    fun read(table: SSTableMetadata, key: String): Record? {
        val ops = operations.incrementAndGet()
        if (ops % 10_000_000 == 0L) {
            logMetrics()
        }

        if (!cache.containsKey(table.id)) {
            cacheMisses.incrementAndGet()
            cache[table.id] = reader.mmap(table)
        }
        return cache[table.id]?.get(key)
    }

    fun read(table: SSTableMetadata): Sequence<Entry> = sequence {
        if (!cache.containsKey(table.id)) {
            cache[table.id] = reader.mmap(table)
        }

        cache[table.id]?.forEach {
            yield(it.first to it.second)
        }
    }

    // TODO (will) split read and write buffers.
    fun write(table: Long, key: String, value: Record?) {
//        val map = cache.getOrPut(table, { TreeMap() })
//        map[key] = value
    }

    /**
     * Create a new SSTable from the log with the given ID. Return its metadata. Because this operation currently
     * requires reading the entire table into memory and sorting it, a semaphore is used to limit the number of threads
     * concurrently performing this function.
     *
     * TODO (will) implement a sort on the raw bytes to avoid object churn
     */
    fun write(logId: Long): SSTableMetadata {
        val writer = createSSTableManager(sstableFileGenerator)
        val data = createLogReader(walFileGenerator.path(logId))
            .readAll()
            .sortedBy { it.first }
        data.forEach {
            writer.append(it.first, it.second)
        }

        val logHandle = writer.rotate()
        return SSTableMetadata(
            path = sstableFileGenerator.path(logHandle.id).toString(),
            minKey = data.first().first,
            maxKey = data.last().first,
            level = 0,
            id = logHandle.id,
            fileSize = logHandle.totalBytes
        )
    }

    fun write(log: MemTable): SSTableMetadata {
        try {
            tableWriteSemaphore.acquire()
            logger.debug("write() started")
            val logManager = createSSTableManager(sstableFileGenerator)
            logManager.use { writer ->
                val result = TreeMap<String, Record?>()
                log.forEach { entry ->
                    result[entry.key] = entry.value
                    writer.append(entry.key, entry.value)
                }
            }

            val logHandle = logManager.rotate()

            // TODO (will) put this back
//                synchronized(cache) {
//                    cache[tableMeta.id] = result
//                }

            return SSTableMetadata(
                path = sstableFileGenerator.path(logHandle.id).toString(),
                minKey = log.first().key,
                maxKey = log.last().key,
                level = 0,
                id = logHandle.id,
                fileSize = logHandle.totalBytes
            )
        } finally {
            logger.debug("write() log=$log finished")
            tableWriteSemaphore.release()
        }
    }

    private fun logMetrics() {
        val hitRate = "%.2f".format((operations.get() - cacheMisses.get()) / operations.get().toDouble())
        logger.info("CachedSSTableReader metrics: operations=${operations.get()} cacheMisses=${cacheMisses.get()} hitRate=$hitRate")
    }

    companion object {
        val logger = KotlinLogging.logger {}
    }
}

class LRUCache<K, V>(private val maxSize: Int) : LinkedHashMap<K, V>(
    maxSize * 3,
    0.5F,
    true
) {
    override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean =
        size > maxSize
}
