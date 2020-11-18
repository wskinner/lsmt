// Keeps track of the SSTable files
interface SSTableManager : LSMRunnable {

    // Get a value from the on-disk storage.
    fun get(key: String): Map<String, Any>?

    // Add a new SSTable.
    fun addTable(table: MemTable)

    // Perform compaction on the existing tables.
    fun doCompaction()
}

class StandardSSTableManager() : SSTableManager {
    override fun get(key: String): Map<String, Any>? {
        TODO("Not yet implemented")
    }

    override fun addTable(table: MemTable) {
        TODO("Not yet implemented")
    }

    override fun doCompaction() {
        TODO("Not yet implemented")
    }

    override fun start() {
        TODO("Not yet implemented")
    }

    override fun stop() {
        TODO("Not yet implemented")
    }

}
