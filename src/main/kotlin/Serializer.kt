import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.*
import java.util.*

interface Serializer {
    fun serialize(memTable: MemTable, file: File)

    fun deserialize(file: File): MemTable

    fun deserializeBloomFilter(file: File): BloomFilter
}

class StandardSerializer : Serializer {
    override fun serialize(memTable: MemTable, file: File) {
        FileOutputStream(file, true).use {
            jacksonObjectMapper().writeValue(file, memTable.storage)
        }
    }

    override fun deserialize(file: File): MemTable {
        val contents = jacksonObjectMapper().readValue(file, TreeMap<String, Map<String, Any>>().javaClass)
        return StandardMemTable(contents)
    }

    override fun deserializeBloomFilter(file: File): BloomFilter {
        TODO("Not yet implemented")
    }

}
