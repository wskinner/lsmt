package com.lsmt.log

import com.lsmt.core.Entry
import com.lsmt.toInt
import com.lsmt.toKey
import java.io.InputStream

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
    fun decode(): Entry {
        val size = readNBytes(4).toInt()
        val isDelete = size < 0
        val keyLength = size and Int.MAX_VALUE
        val key = readNBytes(keyLength).toKey()

        if (isDelete)
            return key to null
        return key to readNBytes(totalBytes - bytesRead)
    }


    override fun readNBytes(len: Int): ByteArray {
        val result = ByteArray(len)
        var resultOffset = 0
        var remaining = len
        while (remaining > 0 && available() > 0) {
            val amount = Integer.min(remaining, arrays[listIndex].size - arrayIndex)
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
