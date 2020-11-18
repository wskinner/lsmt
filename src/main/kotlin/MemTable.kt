interface MemTable : Iterable<MutableMap.MutableEntry<String, Record>> {
    fun put(key: String, record: Record)

    fun get(key: String): Map<String, Any>?

    fun getRecord(key: String): Record?

    fun delete(key: String)

    fun size(): Int

    val storage: MutableMap<String, Record>
}

class StandardMemTable(
    override val storage: MutableMap<String, Record>
) : MemTable {

    override fun put(key: String, record: Record) {
        storage[key] = record
    }

    override fun get(key: String): Map<String, Any>? = storage[key]?.value

    override fun getRecord(key: String): Record? = storage[key]

    override fun delete(key: String) {
        storage.remove(key)
    }

    override fun size() = storage.size

    override fun iterator() = storage.iterator()
}
