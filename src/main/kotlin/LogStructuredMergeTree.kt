import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.max

interface LogStructuredMergeTree : LSMRunnable {
    fun put(key: String, value: Map<String, Any>)

    fun get(key: String): Map<String, Any>?

    fun delete(key: String)
}

class StandardLogStructuredMergeTree(
    private val memTableFactory: () -> MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: WriteAheadLogManager,
    private val maxMemtableSize: Int = 10000
) : LogStructuredMergeTree {

    private var memTable = memTableFactory()

    override fun put(key: String, value: Map<String, Any>) {
        writeAheadLog.append(key, value)
        memTable.put(key, value)

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

}
