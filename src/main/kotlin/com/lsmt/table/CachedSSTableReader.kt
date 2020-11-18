package com.lsmt.table

import com.lsmt.core.Record
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.atomic.AtomicLong

class CachedSSTableReader(
    private val delegate: SSTableReader,
    maxSize: Int = 10
) : SSTableReader {
    val cacheMisses = AtomicLong()
    val operations = AtomicLong()

    private val cache = LRUCache<Int, SortedMap<String, Record?>>(maxSize)

    override fun read(table: SSTableMetadata, key: String): Record? {
        val ops = operations.incrementAndGet()
        if (ops % 1000 == 0L) {
            logMetrics()
        }

        if (!cache.containsKey(table.id)) {
            cacheMisses.incrementAndGet()
            cache[table.id] = delegate.readAll(table)
        }
        return cache[table.id]?.get(key)
    }

    override fun readAll(table: SSTableMetadata): SortedMap<String, Record?> {
        if (!cache.containsKey(table.id)) {
            cache[table.id] = delegate.readAll(table)
        }
        return cache[table.id]!!
    }

    fun logMetrics() {
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
