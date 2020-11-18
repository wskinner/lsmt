import com.dslplatform.json.DslJson
import java.io.*
import java.util.*

interface Serializer {
    fun serialize(memTable: MemTable, file: File)

    fun deserialize(file: File): MemTable

    fun deserializeBloomFilter(file: File): BloomFilter
}

class StandardSerializer : Serializer {
    private val dslJson = DslJson<Any>()

    override fun serialize(memTable: MemTable, file: File) {
        FileOutputStream(file, true).use {
            dslJson.serialize(memTable.storage, file.outputStream().buffered())
        }
    }

    override fun deserialize(file: File): MemTable {
        val contents = dslJson.deserialize(TreeMap<String, Record>().javaClass, file.inputStream())!!
        return StandardMemTable(contents)
    }

    override fun deserializeBloomFilter(file: File): BloomFilter {
        TODO("Not yet implemented")
    }

}
