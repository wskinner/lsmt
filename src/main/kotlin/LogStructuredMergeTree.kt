import log.BinaryWriteAheadLogManager

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
    private val maxMemtableSize: Int = 10000
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    override fun put(key: String, value: Record) {
        writeAheadLog.append(key, value)
        memTable.put(key, value)

        if (memTable.size() > maxMemtableSize) {
            synchronized(memTable) {
                ssTable.addTableAsync(memTable)
                memTable = memTableFactory()
            }
        }
    }

    override fun get(key: String): Record? = memTable.get(key) ?: ssTable.get(key)

    override fun delete(key: String) {
        TODO("Not yet implemented")
    }

    override fun start() {
        writeAheadLog.start()
    }

    override fun close() {
        writeAheadLog.close()
        ssTable.close()
    }

    companion object {
        const val SEQUENCE = "SEQ_nlXo9jaJFBMTjkWyeg"
    }
}
