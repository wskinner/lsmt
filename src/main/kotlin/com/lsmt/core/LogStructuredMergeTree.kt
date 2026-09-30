package com.lsmt.core

import ch.qos.logback.classic.Level
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.log.BinaryLogManager
import com.lsmt.table.SSTableManager
import mu.KotlinLogging
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

interface LogStructuredMergeTree : AutoCloseable {
    fun start()

    fun put(key: Key, value: Record)

    fun get(key: Key): Record?

    fun delete(key: Key)
}

class StandardLogStructuredMergeTree(
    private val memTableFactory: () -> MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: BinaryLogManager,
    private val config: Config = DefaultConfig,
    logLevel: Level = Level.INFO
) : LogStructuredMergeTree {

    private val writeCounter = AtomicLong()
    private val readCounter = AtomicLong()

    private val lock = ReentrantReadWriteLock()
    private val readLock = lock.readLock()
    private val writeLock = lock.writeLock()
    private val closeMonitor = Any()

    private var closed = false
    private var memTable = memTableFactory()

    // Memtables copied to SSTables, newest first. Visible to readers until the table is in the manifest.
    private val flushing = ArrayDeque<MemTable>()

    init {
        val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        rootLogger.level = logLevel
    }

    /**
     * Add a key, value pair to the database. Safe to call concurrently with other reads and writes.
     */
    override fun put(key: Key, value: Record) {
        withWriteLock {
            writeAheadLog.append(key, value)
            memTable.put(key, value)

            if (writeAheadLog.size() > config.maxWalSize) {
                freezeMemTable()
            }
        }

        val count = writeCounter.incrementAndGet()
        if (count % 10_000_000 == 0L) {
            logger.info("Processed $count writes")
        }
    }

    override fun get(key: Key): Record? {
        val memoryHit = readMemory(key)
        val result = memoryHit ?: ssTable.get(key)
        val count = readCounter.incrementAndGet()
        if (count % 10_000_000 == 0L) {
            logger.info("Processed $count reads")
        }
        return result
    }

    override fun delete(key: Key) {
        withWriteLock {
            writeAheadLog.append(key, null)
            memTable.delete(key)
        }
    }

    override fun start() {}

    /**
     * Shut down the tree. After this function returns, All existing log files will have been flushed to SSTables, and
     * the manifest will have been updated.
     */
    override fun close() {
        synchronized(closeMonitor) {
            val active = try {
                writeLock.lock()
                if (closed) return
                closed = true
                val current = memTable
                memTable = memTableFactory()
                if (current.size() > 0) {
                    flushing.addFirst(current)
                    current
                } else {
                    null
                }
            } finally {
                writeLock.unlock()
            }

            logger.info("Shutting down LSMT")
            try {
                if (active != null) {
                    ssTable.addTableFromLog(active)
                    discardFrozen(active)
                }
            } finally {
                writeAheadLog.close()
                ssTable.close()
                logger.info("Done shutting down LSMT")
                logger.info(toString())
            }
        }
    }

    /**
     * Caller holds the write lock. The frozen table stays readable until its SSTable is in the manifest.
     */
    private fun freezeMemTable() {
        val logHandle = writeAheadLog.rotate()
        val frozen = memTable
        memTable = memTableFactory()
        flushing.addFirst(frozen)
        ssTable.addTableAsync(frozen, logHandle.id) {
            discardFrozen(frozen)
        }
    }

    private fun discardFrozen(frozen: MemTable) {
        writeLock.lock()
        try {
            flushing.remove(frozen)
        } finally {
            writeLock.unlock()
        }
    }

    private fun readMemory(key: Key): Record? {
        readLock.lock()
        try {
            memTable.get(key)?.let { return it }
            for (table in flushing) {
                table.get(key)?.let { return it }
            }
            return null
        } finally {
            readLock.unlock()
        }
    }

    private inline fun <T> withWriteLock(block: () -> T): T {
        writeLock.lock()
        try {
            if (closed) {
                throw IllegalStateException("LSM tree is closed")
            }
            return block()
        } finally {
            writeLock.unlock()
        }
    }

    override fun toString(): String = ssTable.toString()

    companion object {
        val logger = KotlinLogging.logger { }
    }
}
