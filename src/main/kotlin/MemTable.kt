interface MemTable {
    fun put(key: String, value: Map<String, Any>)

    fun get(key: String): Map<String, Any>?

    fun delete(key: String)
}

class StandardMemTable(
    private val storage: MutableMap<String, Map<String, Any>>
) : MemTable {

    override fun put(key: String, value: Map<String, Any>) {
        storage[key] = value
    }

    override fun get(key: String): Map<String, Any>? = storage[key]

    override fun delete(key: String) {
        storage.remove(key)
    }
}
