import java.io.File
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

// Keeps track of the SSTable files
interface SSTableManager : LSMRunnable {

    // Get a value from the on-disk storage.
    fun get(key: String): SortedMap<String, Any>?

    // Add a new SSTable.
    fun addTable(memTable: MemTable)

    // Perform compaction on the existing tables.
    fun doCompaction()

    fun addTableAsync(memTable: MemTable)
}

/**
 * SSTable files are arranged in levels. Tables in level zero, the "young" level can have overlapping keys. Tables in
 * other levels must contain no overlapping keys. I.e. for levels greater than zero, a key will be within the key range
 * of at most one table. Since records gradually move from the lower to the higher levels, this implies that for a given
 * key, the record in the lowest level must be the most recent (except for the records in the young level, where overlap
 * may occur).
 */
class StandardSSTableManager(
    // Directory where the SSTable files will be stored
    private val rootDirectory: File,
    private val serializer: Serializer,
    private val manifest: Manifest
) : SSTableManager {
    private val threadPool: ExecutorService =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

    private val tableCounter = AtomicInteger(nextSSTable())

    init {
        if (!rootDirectory.exists()) {
            rootDirectory.mkdir()
        }
    }

    override fun get(key: String): SortedMap<String, Any>? = getYoung(key) ?: getOld(key)

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
        return File(rootDirectory, PREFIX + (tableCounter.getAndIncrement()))
    }

    private fun getYoung(key: String): SortedMap<String, Any>? {
        var maxSeq = 0L
        var value: SortedMap<String, Any>? = null
        // Search level 0
        manifest.tables()[0]?.forEach { tableMeta ->
            if (tableMeta.keyRange.contains(key)) {
                val table = serializer.deserialize(File(rootDirectory, tableMeta.name))
                table.getRecord(key)?.also {
                    if (it.sequence > maxSeq) {
                        value = it.value
                        maxSeq = it.sequence
                    }
                }
            }
        }

        return value
    }

    private fun getOld(key: String): SortedMap<String, Any>? {
        manifest.tables().forEach { (_, tableMetas) ->
            tableMetas.forEach { tableMeta ->
                if (tableMeta.keyRange.contains(key)) {
                    val table = serializer.deserialize(File(rootDirectory, tableMeta.name))
                    val record = table.get(key)
                    if (record != null) return record
                }
            }
        }

        return null
    }

    private fun nextSSTable(): Int {
        var max = 0
        rootDirectory.list { _, name ->
            name?.startsWith(PREFIX) ?: false
        }?.forEach {
            val num = it.removePrefix(PREFIX).toInt()
            max = max(max, num)
        }

        return max
    }

    companion object {
        // File names follow the pattern sstable_1234, where files with larger suffixes were created later.
        const val PREFIX: String = "sstable_"
    }
}
