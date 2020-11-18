package core

import Config
import log.BinaryWriteAheadLogManager
import table.MemTable
import table.SSTableManager
import table.SSTableMetadata

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
    private val config: Config
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    override fun put(key: String, value: Record) = synchronized(this) {
        writeAheadLog.append(key, value)
        memTable.put(key, value)

        var maxRange: KeyRange
        val toMerge = mutableListOf<SSTableMetadata>()
        if (writeAheadLog.size() > config.maxWalSize) {
            ssTable.addTableAsync(writeAheadLog.rotate())
            memTable = memTableFactory()
        }
    }

    override fun get(key: String): Record? = memTable.get(key) ?: ssTable.get(key)

    override fun delete(key: String) {
        TODO("Not yet implemented")
    }

    override fun start() {}

    override fun close() {
        writeAheadLog.close()
        ssTable.close()
    }
}
