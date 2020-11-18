interface LogStructuredMergeTree : LSMRunnable {
    fun put(key: String, value: Map<String, Any>)

    fun get(key: String): Map<String, Any>?

    fun delete(key: String)
}

class StandardLogStructuredMergeTree(
    private val memTable: MemTable,
    private val ssTable: SSTableManager,
    private val writeAheadLog: WriteAheadLogManager
) : LogStructuredMergeTree {

    override fun put(key: String, value: Map<String, Any>) {
        writeAheadLog.append(key, value)
        memTable.put(key, value)
    }

    override fun get(key: String): Map<String, Any>? = memTable.get(key) ?: ssTable.get(key)

    override fun delete(key: String) {
        TODO("Not yet implemented")
    }

    override fun start() {
        TODO("Not yet implemented")
    }

    override fun stop() {
        TODO("Not yet implemented")
    }

}
