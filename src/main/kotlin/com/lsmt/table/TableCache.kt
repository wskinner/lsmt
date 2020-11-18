package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.FileGenerator
import com.lsmt.log.createLogReader
import mu.KotlinLogging
import java.util.*
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
     * Create a new SSTable from the log with the given ID. Return its metadata.
     */
    fun write(logId: Int): SSTableMetadata {
        logger.debug("Writing table for log=$logId")
        val logPath = walFileGenerator.path(logId)
        BinaryLogManager(sstableFileGenerator).use { writer ->
            val data = try {
                val data = createLogReader(logPath).readAll().sortedBy { it.first }
                data
            } catch (e: Throwable) {
                throw e
            }

            val result = TreeMap<String, Record?>()
            var totalBytes = 0
            data.forEach { entry ->
                result[entry.first] = entry.second
                totalBytes += writer.append(entry.first, entry.second)
            }

            val tableMeta = SSTableMetadata(
                path = writer.filePath.toString(),
                minKey = data.first().first,
                maxKey = data.last().first,
                level = 0,
                id = writer.id,
                fileSize = totalBytes
            )

            cache[tableMeta.id] = result
            return tableMeta
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
