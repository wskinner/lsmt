package com.lsmt.log

import com.lsmt.core.checksum
import com.lsmt.domain.Entry
import com.lsmt.toInt
import mu.KotlinLogging
import java.io.InputStream
import java.io.SequenceInputStream
import java.util.zip.CRC32C

/**
 * InputStream that counts the number of bytes that have been read. Not thread safe.
 */
class CountingInputStream(private val delegate: InputStream) : InputStream() {
    private val crc = CRC32C()

    var bytesRead = 0

    private val totalBytes = delegate.available()

    override fun read(): Int {
        bytesRead++
        return delegate.read()
    }

    override fun readNBytes(len: Int): ByteArray {
        bytesRead += len
        return delegate.readNBytes(len)
    }

    override fun readAllBytes(): ByteArray {
        val result = delegate.readAllBytes()
        bytesRead = result.size
        return result
    }

    override fun available(): Int {
        return totalBytes - bytesRead
    }

    /**
     * The writer will only start a new record if there are enough bytes in the current block to write the whole header.
     * Otherwise, it will write a trailer consisting of 0s, starting the next header at the beginning of a new block.
     * Therefore, if there are fewer than 9 bytes remaining, we skip the remaining bytes in this block.
     */
    private fun readTrailer() {
        val remainingBytes = BinaryLogWriter.BLOCK_SIZE - (bytesRead % BinaryLogWriter.BLOCK_SIZE)
        if (remainingBytes < 9) {
            readNBytes(remainingBytes)
        }
    }

    private fun readHeader(): Header {
        readTrailer()
        val crc = readNBytes(4).toInt()
        val length = readNBytes(4).toInt()
        val type = read()

        return Header(crc, length, type)
    }

    private fun readData(header: Header): ByteArray {
        val data = readNBytes(header.length)
        if (crc.checksum(header.type, data) != header.crc) {
            logger.error("Corrupt record")
        }
        return data
    }

    fun readEntry(): Entry {
        var header = readHeader()
        val data = readData(header)

        if (header.type == BinaryLogWriter.FULL) {
            return data.inputStream().decode()
        }

        val allData = mutableListOf(data)
        // It was a first record
        do {
            header = readHeader()
            allData.add(readData(header))
        } while (header.type != BinaryLogWriter.LAST)

        return allData.concat().decode()
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

fun List<ByteArray>.concat(): InputStream = map { it.inputStream() as InputStream }
    .reduce(operation = { acc, inputStream -> SequenceInputStream(acc, inputStream) })
