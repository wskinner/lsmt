package com.lsmt.table

import com.lsmt.log.BinaryLogWriter.Companion.FIRST
import com.lsmt.log.BinaryLogWriter.Companion.FULL
import com.lsmt.log.DELETE_MASK
import com.lsmt.log.Header
import com.lsmt.readHeader
import com.lsmt.toInt
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.lang.Integer.min
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

interface TableIterator {
    /**
     * Search for the key. If found and the record is a deletion, return null. If not found, return false. Otherwise
     * return true.
     */
    fun seek(targetKey: String): Boolean?

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
class StandardTableIterator(
    private val delegate: ByteBuffer,
    private val startIndex: Int,
    private val dataLimit: Int
) : TableIterator {

    init {
        delegate.position(startIndex)
    }

    // Offset of the first header of the record we are currently iterating
    private var currentRecordOffset = 0

    // Header whose data we are currently iterating
    private var currentHeader: Header = seekToNextRecord()

    // Offset into the data array of the current header
    private var currentOffset: Int = 0

    private var currentKeyDelete = readKey()

    /**
     * Increment the position in the buffer until the value of position() is the first data byte of a FIRST or FULL
     * record, or is equal to dataLimit.
     *
     */
    private fun seekToNextRecord(): Header {
        var header = delegate.readHeader()
        while (header.type != FIRST && header.type != FULL && delegate.position() + header.length < dataLimit) {
            delegate.position(delegate.position() + header.length)
            header = delegate.readHeader()
        }
        currentRecordOffset = delegate.position() - 9
        return header
    }

    private fun advance() {
        delegate.position(delegate.position() + currentHeader.length - currentOffset)
        if (delegate.position() < dataLimit) {
            currentOffset = 0
            currentHeader = seekToNextRecord()
            currentKeyDelete = readKey()
        }
    }

    /**
     * Read and return all data until the next header.
     */
    private fun readToNextRecord(): ByteArray {
        val baos = ByteArrayOutputStream()
        baos.writeBytes(readNBytes(currentHeader.length - currentOffset))
        while (currentHeader.type != FIRST && currentHeader.type != FULL) {
            baos.writeBytes(readNBytes(currentHeader.length))
        }
        val result = baos.toByteArray()

        return result
    }

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
            currentOffset += amount
            remaining -= amount
            resultOffset += amount
            if (currentOffset == currentHeader.length && delegate.position() < dataLimit) {
                currentHeader = delegate.readHeader()
                currentOffset = 0
            }
        }

        return result
    }

    private fun readKey(): Pair<String, Boolean> {
        val size = readNBytes(4).toInt()
        val isDelete = size and DELETE_MASK < 0
        val keySize = size and Integer.MAX_VALUE
        val key = readNBytes(keySize).decodeToString()
        return key to isDelete
    }

    override fun seek(targetKey: String): Boolean? {
        totalReads.getAndIncrement()
        while (currentKeyDelete.first != targetKey && delegate.position() < dataLimit)
            advance()

        return if (currentKeyDelete.first == targetKey) {
            if (currentKeyDelete.second) {
                null
            } else {
                true
            }
        } else {
            false
        }
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
    }
}
