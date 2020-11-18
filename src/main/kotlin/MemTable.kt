interface MemTable : Iterable<MutableMap.MutableEntry<String, Map<String, Any>>> {
    fun put(key: String, value: Map<String, Any>)

    fun get(key: String): Map<String, Any>?

    fun delete(key: String)

    fun size(): Int

    val storage: MutableMap<String, Map<String,Any>>
}

class StandardMemTable(
    override val storage: MutableMap<String, Map<String, Any>>
) : MemTable {

    override fun put(key: String, value: Map<String, Any>) {
        storage[key] = value
    }

    override fun get(key: String): Map<String, Any>? = storage[key]

    override fun delete(key: String) {
        storage.remove(key)
    }

    override fun size() = storage.size

    override fun iterator() = storage.iterator()
}
