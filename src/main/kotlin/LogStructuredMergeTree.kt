
interface LogStructuredMergeTree : LSMRunnable {
    fun put(key: String, value: Map<String, Any>)

    fun get(key: String): Map<String, Any>?

    fun delete(key: String)
}

data class Record(
    val sequence: Long,
    val value: Map<String, Any>
)

class StandardLogStructuredMergeTree(
    private val memTableFactory: () -> MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: WriteAheadLogManager,
    private val maxMemtableSize: Int = 10000
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    override fun put(key: String, value: Map<String, Any>) {
        val seq = writeAheadLog.append(key, value)
        memTable.put(key, Record(seq, value))

        if (memTable.size() > maxMemtableSize) {
            synchronized(memTable) {
                ssTable.addTableAsync(memTable)
                memTable = memTableFactory()
            }
        }
    }

    override fun get(key: String): Map<String, Any>? = memTable.get(key) ?: ssTable.get(key)

    override fun delete(key: String) {
        TODO("Not yet implemented")
    }

    override fun start() {
        writeAheadLog.start()
        ssTable.start()
    }

    override fun stop() {
        writeAheadLog.stop()
        ssTable.stop()
    }

    companion object {
        const val SEQUENCE = "SEQ_nlXo9jaJFBMTjkWyeg"
    }
}
