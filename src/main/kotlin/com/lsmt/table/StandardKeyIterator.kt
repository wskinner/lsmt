package com.lsmt.table

import com.lsmt.domain.Key
import com.lsmt.log.BinaryLogWriter.Companion.FIRST
import com.lsmt.log.BinaryLogWriter.Companion.FULL
import com.lsmt.log.DELETE_MASK
import com.lsmt.log.Header
import com.lsmt.readHeader
import com.lsmt.toInt
import com.lsmt.toKey
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.lang.Integer.min
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.zip.CRC32C

interface KeyIterator {
    /**
     * Search for the key. If found and the record is a deletion, return null. If not found, return false. Otherwise
     * return true.
     */
    fun seek(targetKey: Key): Boolean?

    /**
     * Return the value associated with the key. Caller must have previously called seek(), and seek() must have
     * returned true.
     */
    fun read(): ByteArray

    fun currentRecordOffset(): Int
}

/**
 * Expose the bytes in the sub-buffer starting at startIndex as a lazy stream.
 */
class StandardKeyIterator(
    private val delegate: ByteBuffer,
    private val startIndex: Int,
    private val dataLimit: Int
) : KeyIterator {

    init {
        delegate.position(startIndex)
    }

    private val crc = CRC32C()

    // Offset of the first header of the record we are currently iterating
    private var currentRecordOffset = 0

    // Header whose data we are currently iterating
    private var currentHeader: Header = seekToNextRecord()

    // Offset into the data array of the current header
    private var currentOffset: Int = 0

    private var currentKeyAndSize = readKey()

    /**
     * Increment the position in the buffer until the value of position() is the first data byte of a FIRST or FULL
     * record, or is equal to dataLimit.
     *
     */
    private fun seekToNextRecord(): Header {
        var header = readHeader()
        while (header.type != FIRST && header.type != FULL && delegate.position() + header.length < dataLimit) {
            delegate.position(delegate.position() + header.length)
            header = readHeader()
        }
        currentRecordOffset = delegate.position() - 9
        return header
    }

    private fun advance() {
        delegate.position(delegate.position() + currentHeader.length - currentOffset)
        if (delegate.position() < dataLimit) {
            currentOffset = 0
            currentHeader = seekToNextRecord()
            currentKeyAndSize = readKey()
        }
    }

    /**
     * Read and return all data until the next header. At this point, we've already read the first header, key size, and
     * key.
     */
    private fun readToNextRecord(): ByteArray {
        val baos = ByteArrayOutputStream()
        baos.writeBytes(readNBytes(currentHeader.length - currentOffset))
        while (currentHeader.type != FIRST && currentHeader.type != FULL) {
            val bytes = readNBytes(currentHeader.length)
            baos.writeBytes(bytes)
        }

        return baos.toByteArray()
    }

    private fun verifyChecksum() {
        if (crc.value.toInt() != currentHeader.crc)
            logger.error("Corrupt record")
    }

    /**
     * Read some bytes from the data portion of the record.
     */
    private fun readNBytes(len: Int): ByteArray {
        val result = ByteArray(len)
        var resultOffset = 0
        var remaining = len
        while (remaining > 0) {
            val amount = min(remaining, currentHeader.length - currentOffset)
            delegate.get(
                result,
                resultOffset,
                amount
            )
            crc.update(result, resultOffset, amount)
            currentOffset += amount
            remaining -= amount
            resultOffset += amount
            if (currentOffset == currentHeader.length && delegate.position() < dataLimit) {
                verifyChecksum()
                currentHeader = readHeader()
                currentOffset = 0
            }
        }

        return result
    }

    private fun readKey(): Pair<Key, Int> {
        val size = readNBytes(4)
        val sizeInt = size.toInt()
        val key = readNBytes(sizeInt.keySize())
        return key.toKey() to sizeInt
    }

    override fun seek(targetKey: Key): Boolean? {
        totalReads.getAndIncrement()
        while (currentKeyAndSize.first != targetKey && delegate.position() < dataLimit)
            advance()

        return if (currentKeyAndSize.first == targetKey) {
            if (currentKeyAndSize.second.isDelete()) {
                null
            } else {
                true
            }
        } else {
            false
        }
    }

    private fun readHeader(): Header {
        val result = delegate.readHeader()
        crc.reset()
        crc.update(result.type)
        return result
    }

    override fun read(): ByteArray {
        val result = readToNextRecord()
        totalSeekBytes.getAndAdd((delegate.position() - startIndex).toLong())
        return result
    }

    override fun currentRecordOffset(): Int = currentRecordOffset

    companion object {
        val totalSeekBytes = AtomicLong()
        val totalReads = AtomicLong()
        val logger = KotlinLogging.logger { }
    }
}

fun Int.isDelete(): Boolean = this and DELETE_MASK < 0

fun Int.keySize() = this and Integer.MAX_VALUE
