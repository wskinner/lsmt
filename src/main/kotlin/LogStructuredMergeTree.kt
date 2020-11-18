import java.util.*

interface LogStructuredMergeTree : AutoCloseable {
    fun start()

    fun put(key: String, value: SortedMap<String, Any>)

    fun get(key: String): SortedMap<String, Any>?

    fun delete(key: String)
}

data class Record(
    val sequence: Long,
    val value: SortedMap<String, Any>
)

class StandardLogStructuredMergeTree(
    private val memTableFactory: () -> MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: WriteAheadLogManager,
    private val maxMemtableSize: Int = 10000
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    override fun put(key: String, value: SortedMap<String, Any>) {
        val seq = writeAheadLog.append(key, value)
        memTable.put(key, Record(seq, value))

        if (memTable.size() > maxMemtableSize) {
            synchronized(memTable) {
                ssTable.addTableAsync(memTable)
                memTable = memTableFactory()
            }
        }
    }

    override fun get(key: String): SortedMap<String, Any>? = memTable.get(key) ?: ssTable.get(key)

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
