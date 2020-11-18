package com.lsmt.table

import com.lsmt.core.*
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.BinaryLogWriter.Companion.FIRST
import com.lsmt.log.BinaryLogWriter.Companion.FULL
import com.lsmt.log.BinaryLogWriter.Companion.LAST
import com.lsmt.log.DELETE_MASK
import com.lsmt.log.Header
import com.lsmt.readInt
import com.lsmt.readString
import com.lsmt.toInt
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

interface TableBuffer {
    fun get(key: String): Record?

    fun position(): Int

    fun limit(): Int

    fun iterator(): Iterator<Entry>
}

class StandardTableBuffer(
    val delegate: ByteBuffer,
    val table: SSTableMetadata
) : TableBuffer {

    private val blockIndex = readBlockIndex()
    private val dataLimit = dataLength()

    init {
        delegate.order(ByteOrder.LITTLE_ENDIAN)
        delegate.position(0)
    }

    private val crc = CRC32C()

    override fun get(key: String): Record? {
        val closestBlock = blockIndex.floorEntry(key).value
        return if (closestBlock == null) {
            null
        } else {
            getInBlock(key, closestBlock)
        }
    }

    /**
     * The writer will only start a new record if there are enough bytes in the current block to write the whole header.
     * Otherwise, it will write a trailer consisting of 0s, starting the next header at the beginning of a new block.
     * Therefore, if there are fewer than 9 bytes remaining, we skip the remaining bytes in this block.
     */
    private fun readTrailer() {
        var remainingBytes = BinaryLogWriter.BLOCK_SIZE - (delegate.position() % BinaryLogWriter.BLOCK_SIZE)
        if (remainingBytes < 9) {
            while (remainingBytes > 0) {
                delegate.get()
                remainingBytes--
            }
        }
    }

    private fun readInt(): Int = readNBytes(4).toInt()

    private fun readString(length: Int): String = readNBytes(length).decodeToString()

    private fun readType(): Byte = delegate.get()

    private fun readHeader(): Header {
        readTrailer()
        val crc = readInt()
        val length = readInt()
        val type = readType()
        return Header(crc, length, type.toInt())
    }

    private fun readNBytes(length: Int): ByteArray {
        val result = ByteArray(length)
        delegate.get(result)
        return result
    }

    /**
     * Increment the position in the buffer until the value of position() is the first data byte of a FIRST or FULL
     * record, or is equal to dataLimit.
     */
    private fun seekToNextRecord(): Header? {
        while (position() < dataLimit) {
            val header = readHeader()
            if (header.type == FIRST || header.type == FULL)
                return header

            delegate.position(position() + header.length)
        }
        return null
    }

    /**
     * Seek to the beginning of the next record and read its bytes
     */
    private fun readRecordBytes(): ByteArray? {
        val baos = ByteArrayOutputStream()
        var header = seekToNextRecord() ?: return null

        while (true) {
            val data = readNBytes(header.length)
            val check = crc.checksum(header.type, data)
            if (check != header.crc) {
                logger.error("Corrupt record file=${table.path}")
            }
            baos.writeBytes(data)
            readTrailer()
            if (header.type == LAST || header.type == FULL)
                return baos.toByteArray()
            header = readHeader()
        }

    }

    private fun readIndexEntry(): Pair<String, BlockHandle> {
        val offset = readInt()
        val length = readInt()
        val keyLength = readInt()
        val key = readString(keyLength)
        return key to BlockHandle(offset, length)
    }

    private fun readBlockIndex(): BlockIndex {
        val indexStart = dataLength()
        val index = BlockIndex()
        delegate.position(indexStart)
        while (delegate.position() < delegate.limit() - 4) {
            val indexEntry = readIndexEntry()
            index[indexEntry.first] = indexEntry.second
        }

        return index
    }

    private fun dataLength(): Int {
        val currentPosition = delegate.position()
        delegate.position(delegate.limit() - 4)
        val length = readInt()
        delegate.position(currentPosition)
        return length
    }

    override fun iterator(): Iterator<Entry> = SSTableIterator(delegate, dataLength())

    /**
     * Search a block for the given key. If there is a record with this key in this block,  return the value. If there
     * is no record, or the record is a delete, return null.
     *
     * TODO (will) optimize this a bit.
     */
    private fun getInBlock(targetKey: String, block: BlockHandle): Record? {
        delegate.position(block.offset)
        while (delegate.position() < block.offset + block.length) {
            val stream = (readRecordBytes() ?: return null).inputStream()
            val size = stream.readInt()
            val isDelete = size and DELETE_MASK < 0
            val keySize = size and Integer.MAX_VALUE
            val key = stream.readString(keySize)
            if (key == targetKey) {
                if (isDelete)
                    return null
                return stream.readAllBytes()
            }
        }

        return null
    }

    override fun position(): Int = delegate.position()

    override fun limit(): Int = delegate.limit()

    companion object {
        val logger = KotlinLogging.logger {}
    }
}

