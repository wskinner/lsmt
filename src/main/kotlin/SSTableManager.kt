import java.io.File
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.math.max

// Keeps track of the SSTable files
interface SSTableManager : LSMRunnable {

    // Get a value from the on-disk storage.
    fun get(key: String): Map<String, Any>?

    // Add a new SSTable.
    fun addTable(memTable: MemTable)

    // Perform compaction on the existing tables.
    fun doCompaction()

    fun addTableAsync(memTable: MemTable)
}

class StandardSSTableManager(
    // Directory where the SSTable files will be stored
    private val rootDirectory: File,
    private val serializer: Serializer
) : SSTableManager {
    private val threadPool: ExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

    init {
        if (!rootDirectory.exists()) {
            rootDirectory.mkdir()
        }
    }

    override fun get(key: String): Map<String, Any>? {
        TODO("Not yet implemented")
    }

    override fun addTable(memTable: MemTable) {
        val file = nextTableFile()
        serializer.serialize(memTable, file)
    }

    override fun doCompaction() {
        TODO("Not yet implemented")
    }

    override fun addTableAsync(memTable: MemTable) {
        threadPool.submit {
            addTable(memTable)
        }
    }

    override fun start() {
    }

    override fun stop() {
        threadPool.shutdown()
    }

    private fun nextTableFile(): File = synchronized(this) {
        var max = 0
        rootDirectory.list { _, name ->
            name?.startsWith(PREFIX) ?: false
        }?.forEach {
            val num = it.removePrefix(PREFIX).toInt()
            max = max(max, num)
        }

        return File(rootDirectory, PREFIX + (max + 1))
    }

    companion object {
        // File names follow the pattern sstable_1234, where files with larger suffixes were created later.
        const val PREFIX: String = "sstable_"
    }
}
