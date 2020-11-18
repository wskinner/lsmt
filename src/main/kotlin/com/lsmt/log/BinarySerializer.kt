package com.lsmt.log

import com.lsmt.core.Record
import com.lsmt.toByteArray
import com.lsmt.toInt
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.lang.Integer.min

const val DELETE_MASK = 1 shl 31

/**
 * This binary format is inspired by the LevelDB log format as described in https://github.com/google/leveldb/blob/master/doc/log_format.md
 *
 * Keys are strings. Values are byte arrays.
 *
 * The most significant bit of the key is used to denote deletions.
 *
 * Each key-value pair is encoded as a record.
 * entry := header record
 *
 * header :=
 * delete := boolean
 * key length := uint31
 * key := uint8[key length]
 * value length := uint32
 *
 * record := uint8[value length]
 *
 * @param key string key
 * @param record if null, this operation is a delete
 */
fun encode(key: String, record: Record?): ByteArray {
    val baos = ByteArrayOutputStream()
    val keyBytes = key.toByteArray()

    var size = keyBytes.size
    if (record == null) {
        size = size or DELETE_MASK
        baos.write(size.toByteArray())
        baos.write(keyBytes)
    } else {
        baos.write(size.toByteArray())
        baos.write(keyBytes)
        baos.write(record)
    }
    return baos.toByteArray()
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

    /**
     * Decode the next KV pair from the stream. If the pair represents a deletion, the second value will be null.
     */
    fun decode(): Pair<String, Record?> {
        val size = readNBytes(4).toInt()
        val isDelete = size < 0
        val keyLength = size and Int.MAX_VALUE
        val key = readNBytes(keyLength).decodeToString()

        if (isDelete)
            return key to null
        return key to readNBytes(totalBytes - bytesRead)
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
