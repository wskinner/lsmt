package com.lsmt.log

import com.lsmt.core.checksum
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.toByteArray
import java.io.OutputStream
import java.util.zip.CRC32C
import kotlin.math.min

/**
 * Implements a binary protocol based on the one used by LevelDB. After append() returns, the
 *
 */
open class BinaryLogWriter(
    protected val os: OutputStream,
    private val flushAfterAppend: Boolean = false
) : LogWriter {
    protected val crc = CRC32C()
    protected var totalBytes: Int = 0
    protected var closed = false

    override fun append(key: Key, value: Record?): Int {
        // A header is always 9 bytes.
        val data = encode(key, value)
        if (flushAfterAppend)
            os.flush()
        return appendBytes(data)
    }


    private fun appendBytes(data: ByteArray): Int {
        val startingBytes = totalBytes
        // The number of data bytes to write during the next call to write()
        var length: Int
        // The checksum
        var check: Int
        // The record type
        var type: Int
        // The offset in the data array. We write the bytes data[offset:offset + length]
        var offset = 0

        length = data.size
        var remainingBytes = writeTrailer()

        if (length <= remainingBytes - 9) {
            // If we can fit the full data array and the header in the current block, do it.
            type = FULL
            check = crc.checksum(type, data)
            write(check, length, type, data, offset)
        } else {
            // The data and header won't fit in the current block. Write the 9 byte header, and a portion of the data.
            length = remainingBytes - 9
            type = FIRST
            check = crc.checksum(type, data, offset, length)
            write(check, length, type, data, offset)

            do {
                remainingBytes = writeTrailer()

                // Here, length is the number of bytes written in the last write.
                offset += length

                // We'll either write all the remaining bytes, or write to the end of the current block.
                length = min(remainingBytes - 9, data.size - offset)

                type = if (data.size > offset + length) {
                    MIDDLE
                } else {
                    LAST
                }

                check = crc.checksum(type, data, offset, length)
                write(check, length, type, data, offset)
            } while (data.size > offset + length)
        }
        return totalBytes - startingBytes
    }

    override fun size(): Int = totalBytes

    override fun totalBytes(): Int = totalBytes

    override fun close() {
        if (!closed) {
            os.flush()
            os.close()
            closed = true
        }
    }

    /**
     * Write the given contents into the log, and return the number of bytes written.
     */
    private fun write(check: Int, length: Int, type: Int, data: ByteArray, offset: Int): Int {
        val checkBytes = check.toByteArray()
        val lengthBytes = length.toByteArray()
        os.write(checkBytes)
        os.write(lengthBytes)
        os.write(type)
        os.write(data, offset, length)
        val bytes = checkBytes.size + lengthBytes.size + 1 + length
        totalBytes += bytes
        return bytes
    }

    /**
     * If needed, write whitespace to fill the end of the block. Return the number of bytes remaining in the current
     * block.
     */
    private fun writeTrailer(): Int {
        val remainingBytes = BLOCK_SIZE - (totalBytes % BLOCK_SIZE)
        if (remainingBytes < 9) {
            repeat(remainingBytes) {
                os.write(0)
                totalBytes++
            }
        }
        return BLOCK_SIZE - (totalBytes % BLOCK_SIZE)
    }

    companion object {
        const val BLOCK_SIZE = 1 shl 15
        const val FULL = 1
        const val FIRST = 2
        const val MIDDLE = 3
        const val LAST = 4
    }

}
