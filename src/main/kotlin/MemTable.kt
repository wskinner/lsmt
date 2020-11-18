import java.util.*

interface MemTable : Iterable<MutableMap.MutableEntry<String, Record>> {
    fun put(key: String, value: Record)

    fun get(key: String): Record?

    fun getRecord(key: String): Record?

    fun delete(key: String)

    fun size(): Int

    val storage: MutableMap<String, Record>
}

class StandardMemTable(
    override val storage: SortedMap<String, Record>
) : MemTable {

    override fun put(key: String, value: Record) {
        storage[key] = value
    }

    override fun get(key: String): Record? = storage[key]

    override fun getRecord(key: String): Record? = storage[key]

    override fun delete(key: String) {
        storage.remove(key)
    }

    override fun size() = storage.size

    override fun iterator() = storage.iterator()
}
