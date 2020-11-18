package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.FileGenerator
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong

class TableCache(
    private val reader: BinarySSTableReader,
    maxSizeMB: Int,
    private val walFileGenerator: FileGenerator,
    private val sstableFileGenerator: FileGenerator
) {

    private val cacheMisses = AtomicLong()
    private val operations = AtomicLong()

    // Estimate the size of a table in memory using the raw disk size.
    private val cache = LRUCache<Int, SortedMap<String, Record?>>(maxSizeMB / 2)

    private val tableWriteSemaphore = Semaphore(10, true)

    fun read(table: SSTableMetadata, key: String): Record? {
        val ops = operations.incrementAndGet()
        if (ops % 1000 == 0L) {
            logMetrics()
        }

        if (!cache.containsKey(table.id)) {
            cacheMisses.incrementAndGet()
            cache[table.id] = reader.readAll(table)
        }
        return cache[table.id]?.get(key)
    }

    fun read(table: SSTableMetadata): Sequence<Entry> = sequence {
        if (!cache.containsKey(table.id)) {
            cache[table.id] = reader.readAll(table)
        }

        cache[table.id]?.forEach {
            yield(it.key to it.value)
        }
    }

    fun write(table: Int, key: String, value: Record?) {
        val map = cache.getOrPut(table, { TreeMap() })
        map[key] = value
    }

    /**
     * Create a new SSTable from the log with the given ID. Return its metadata. Because this operation currently
     * requires reading the entire table into memory and sorting it, a semaphore is used to limit the number of threads
     * concurrently performing this function.
     *
     * TODO (will) implement a sort on the raw bytes to avoid object churn
     */
    fun write(logId: Int): SSTableMetadata {
        TODO()
    }

    private fun logMetrics() {
        val hitRate = "%.2f".format((operations.get() - cacheMisses.get()) / operations.get().toDouble())
        logger.info("CachedSSTableReader metrics: operations=${operations.get()} cacheMisses=${cacheMisses.get()} hitRate=$hitRate")
    }

    fun write(log: MemTable): SSTableMetadata {
        try {
            tableWriteSemaphore.acquire()
            logger.debug("write() started")
            BinaryLogManager(sstableFileGenerator).use { writer ->
                val result = TreeMap<String, Record?>()
                var totalBytes = 0
                log.forEach { entry ->
                    result[entry.key] = entry.value
                    totalBytes += writer.append(entry.key, entry.value)
                }

                val tableMeta = SSTableMetadata(
                    path = writer.filePath.toString(),
                    minKey = log.first().key,
                    maxKey = log.last().key,
                    level = 0,
                    id = writer.id,
                    fileSize = totalBytes
                )

                synchronized(cache) {
                    cache[tableMeta.id] = result
                }
                return tableMeta
            }
        } finally {
            logger.debug("write() log=$log finished")
            tableWriteSemaphore.release()
        }
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
