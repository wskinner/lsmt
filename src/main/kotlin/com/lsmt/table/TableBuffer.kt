package com.lsmt.table

import com.lsmt.core.*
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.BinaryLogWriter.Companion.FULL
import com.lsmt.log.BinaryLogWriter.Companion.LAST
import com.lsmt.log.Header
import com.lsmt.toInt
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.zip.CRC32C

interface TableBuffer {
    fun get(key: String): Record?

    fun position(): Int

    fun limit(): Int

    fun readEntry(): Entry

    fun iterator(): Iterator<Entry>
}

class StandardTableBuffer(
    val delegate: ByteBuffer,
    val table: SSTableMetadata
) : TableBuffer {

    private val blockIndex = readBlockIndex()

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

    fun nextKey(): Pair<Header, Key> {
        val header = readHeader()
        val key = readKey()
        return header to key
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

    private fun readKey(): Key {
        val size = readInt()
        val keyLength = size and Int.MAX_VALUE
        val value = readNBytes(keyLength)
        return Key(size, value)
    }

    private fun readFirstValue(header: Header, key: Key): ByteArray {
        val result = readNBytes(header.length - 4 - key.keyLength)

        val check = crc.checksum(
            type = header.type,
            size = key.size,
            key = key.value,
            value = result
        )
        if (check != header.crc) {
            logger.error("Corrupt record file=${table.path}")
        }
        return result
    }

    private fun readValue(header: Header): ByteArray {
        val result = readNBytes(header.length)
        val check = crc.checksum(header.type, result)
        if (check != header.crc) {
            logger.error("Corrupt record file=${table.path}")
        }
        return result
    }

    /**
     * Before calling this function, the header and key have been read. This function skips the value associated with
     * the header that was read. After calling this function, the buffer's position is equal to the first byte of the
     * next header.
     */
    private fun skipRecord(firstHeader: Header, firstKey: Key) {
        if (firstHeader.type == FULL) {
            val bytesToSkip = firstHeader.length - 4 - firstKey.keyLength
            delegate.position(position() + bytesToSkip)
        } else {
            val bytesToSkip = firstHeader.length - 4 - firstKey.keyLength
            delegate.position(position() + bytesToSkip)
            do {
                val header = readHeader()
                delegate.position(position() + header.length)
            } while (header.type != LAST)
        }
    }

    /**
     * Before calling this function, the header and key have been read. Read and return the value, which may span
     * multiple blocks.
     */
    private fun readRecord(firstHeader: Header, firstKey: Key): Record {
        val firstData = readFirstValue(firstHeader, firstKey)

        if (firstHeader.type == FULL) {
            return firstData
        }

        val os = ByteArrayOutputStream()
        os.write(firstData)

        // It was a first record
        do {
            val header = readHeader()
            val data = readValue(header)
            os.write(data)
        } while (header.type != LAST)

        return os.toByteArray()
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

    override fun readEntry(): Entry {
        val header = readHeader()
        val key = readKey()
        if (key.isDelete)
            return key.key to null

        val record = readRecord(header, key)
        return key.key to record
    }

    override fun iterator(): Iterator<Entry> = SSTableIterator(delegate, dataLength())

    private fun getInBlock(targetKey: String, block: BlockHandle): Record? {
        delegate.position(block.offset)
        while (delegate.position() < block.offset + block.length) {
            val (header, key) = nextKey()
            if (key.key == targetKey) {
                if (key.isDelete)
                    return null
                return readRecord(header, key)
            } else {
                skipRecord(header, key)
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

class ByteBufferInputStream(private val delegate: ByteBuffer) : InputStream() {
    override fun read(): Int {
        return delegate.get().toInt()
    }

    override fun readNBytes(len: Int): ByteArray {
        val result = ByteArray(len)
        delegate.get(result)
        return result
    }

    override fun readAllBytes(): ByteArray {
        val length = delegate.limit() - delegate.position()
        return readNBytes(length)
    }

    override fun available(): Int {
        return delegate.limit() - delegate.position()
    }
}
