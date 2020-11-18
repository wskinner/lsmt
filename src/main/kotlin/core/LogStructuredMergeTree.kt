package core

import Config
import ch.qos.logback.classic.Level
import log.BinaryWriteAheadLogManager
import mu.KotlinLogging
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import table.MemTable
import table.SSTableManager

interface LogStructuredMergeTree : AutoCloseable {
    fun start()

    fun put(key: String, value: Record)

    fun get(key: String): Record?

    fun delete(key: String)
}

class StandardLogStructuredMergeTree(
    private val memTableFactory: () -> MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: BinaryWriteAheadLogManager,
    private val config: Config,
    private val logLevel: Level = Level.INFO
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    init {
        val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        rootLogger.level = logLevel
    }

    /**
     * Add a key, value pair to the database. Not thread safe.
     */
    override fun put(key: String, value: Record) {
        writeAheadLog.append(key, value)
        memTable.put(key, value)

        if (writeAheadLog.size() > config.maxWalSize) {
            ssTable.addTableAsync(writeAheadLog.rotate())
            memTable = memTableFactory()
        }
    }

    override fun get(key: String): Record? = memTable.get(key) ?: ssTable.get(key)

    override fun delete(key: String) {
        writeAheadLog.append(key, null)
        memTable.delete(key)
    }

    override fun start() {}

    /**
     * Shut down the tree. After this function returns, All existing log files will have been flushed to SSTables, and
     * the manifest will have been updated.
     */
    override fun close() {
        logger.info("Shutting down LSMT")
        ssTable.addTableAsync(writeAheadLog.rotate())
        writeAheadLog.close()
        ssTable.close()
        logger.info("Done shutting down LSMT")
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}
