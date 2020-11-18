package com.lsmt.log

import com.lsmt.core.Record
import com.lsmt.toByteArray
import com.lsmt.toDouble
import com.lsmt.toFloat
import com.lsmt.toInt
import com.lsmt.toLong
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.Integer.min
import java.util.*

const val DELETE_MASK = 1 shl 31

/**
 * This binary format is inspired by the LevelDB log format as described in https://github.com/google/leveldb/blob/master/doc/log_format.md
 * Values can be String, Int, Long, Float, or Double.
 *
 * Keys are all strings. Values can be String, Int, Long, Float, or Double.
 *
 * The most significant bit of the key is used to denote deletions.
 *
 * Each key-value pair is encoded as a record.
 * entry := header record*
 *
 * header :=
 * delete := boolean
 * key length := uint31
 * key := uint8[key length]
 *
 * record :=
 * key length := uint31
 * type := uint8
 * value length := int32
 * key := uint8[key length]
 * value := uint8[value length]
 *
 * @param key string key
 * @param record if null, this operation is a delete
 */
fun encode(key: String, record: Record?): ByteArray {
    val baos = ByteArrayOutputStream()
    var keyBytes = key.toByteArray()

    var size = keyBytes.size
    if (record == null) {
        size = size or DELETE_MASK
        baos.write(size.toByteArray())
        baos.write(keyBytes)
    } else {
        baos.write(size.toByteArray())
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
    }
    return baos.toByteArray()
}

/**
 * Decode the next KV pair from the stream. If the pair represents a deletion, the second value will be null.
 */
fun InputStream.decode(): Pair<String, Record?> {
    val size = readNBytes(4).toInt()
    val isDelete = size < 0
    val keyLength = size and Int.MAX_VALUE

    val key = readNBytes(keyLength).decodeToString()

    if (isDelete)
        return key to null

    val map = TreeMap<String, Any>()
    while (available() > 0) {
        val record = readMapRecord()
        map[record.first] = record.second
    }

    return key to map
}

// TODO optimize the format so that only string values include a valueLength field. Saves 4 bytes per non-string
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

    override fun readNBytes(len: Int): ByteArray {
        val result = ByteArray(len)
        var resultOffset = 0
        var remaining = len
        while (remaining > 0 && available() > 0) {
            val amount = min(remaining, arrays[listIndex].size - arrayIndex)
            arrays[listIndex].copyInto(
                result,
                destinationOffset = resultOffset,
                startIndex = arrayIndex,
                endIndex = arrayIndex + amount
            )
            arrayIndex += amount
            bytesRead += amount
            remaining -= amount
            resultOffset += amount
            if (arrayIndex >= arrays[listIndex].size) {
                listIndex++
                arrayIndex = 0
            }
        }

        return result
    }

    override fun readAllBytes(): ByteArray {
        val result = ByteArray(totalBytes)
        var start = 0
        for (arr in arrays) {
            arr.copyInto(result, destinationOffset = start)
            start += arr.size
        }

        bytesRead = totalBytes

        return result
    }

    override fun available(): Int {
        return totalBytes - bytesRead
    }
}

class SerializationException : RuntimeException()

class DeserializationException : RuntimeException()
