package log

import core.Record
import toByteArray
import toDouble
import toFloat
import toInt
import toLong
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.*

/**
 * Implementation of the LevelDB log format as described in https://github.com/google/leveldb/blob/master/doc/log_format.md
 * Values can be String, Int, Long, Float, or Double
 * Format:
 *   length: uint32
 *
 *
 *
 * Keys are all strings. Values can be String, Int, Long, Float, or Double.
 *
 * The map is encoded to a block.
 *
 * Each key-value pair is encoded as a record.
 * entry := header record*
 *
 * header :=
 * key length := int32
 * key := uint8[key length]
 *
 * record :=
 * key length := int32
 * type := uint8
 * value length := int32
 * key := uint8[key length]
 * value := uint8[value length]
 */
fun encode(key: String, record: Record): ByteArray {
    val baos = ByteArrayOutputStream()
    var keyBytes = key.toByteArray()
    baos.write(keyBytes.size.toByteArray())
    baos.write(keyBytes)

    record.forEach { entry ->
        // First write the header, including placeholders
        keyBytes = entry.key.toByteArray()
        val valueBytes = when (val value = entry.value) {
            is String -> value.toByteArray()
            is Int -> value.toByteArray()
            is Long -> value.toByteArray()
            is Float -> value.toByteArray()
            is Double -> value.toByteArray()
            else -> throw SerializationException()
        }

        baos.write(keyBytes.size.toByteArray())
        baos.write(valueType(entry.value).toInt())
        baos.write(valueBytes.size.toByteArray())
        baos.writeBytes(keyBytes)
        baos.writeBytes(valueBytes)
    }

    return baos.toByteArray()
}

fun InputStream.decode(): Pair<String, Record> {
    val keyLength = readNBytes(4).toInt()
    val key = readNBytes(keyLength).decodeToString()
    val map = TreeMap<String, Any>()
    while (available() > 0) {
        val record = readMapRecord()
        map[record.first] = record.second
    }

    return key to map
}

fun InputStream.readMapRecord(): Pair<String, Any> {
    val keyLength = readNBytes(4).toInt()
    val type = read()
    val valueLength = readNBytes(4).toInt()
    val key = readNBytes(keyLength).decodeToString()
    val valueBytes = readNBytes(valueLength)
    val value = when (type) {
        1 -> valueBytes.decodeToString()
        2 -> valueBytes.toInt()
        3 -> valueBytes.toLong()
        4 -> valueBytes.toFloat()
        5 -> valueBytes.toDouble()
        else -> throw DeserializationException()
    }

    return key to value
}

// 99610137

fun valueType(value: Any): Byte = when (value) {
    is String -> 1
    is Int -> 2
    is Long -> 3
    is Float -> 4
    is Double -> 5
    else -> throw SerializationException()
}

class ArraysInputStream(private val arrays: List<ByteArray>) : InputStream() {
    private val totalBytes = arrays.fold(0) { count, r -> count + r.size }
    var bytesRead = 0
    var listIndex = 0
    var arrayIndex = 0

    override fun read(): Int {
        if (available() == 0)
            return -1;

        val byte = arrays[listIndex][arrayIndex++]
        if (arrayIndex == arrays[listIndex].size) {
            listIndex++
            arrayIndex = 0
        }
        bytesRead++
        return byte.toInt()
    }

    override fun available(): Int {
        return totalBytes - bytesRead
    }
}

class SerializationException : RuntimeException()

class DeserializationException : RuntimeException()
