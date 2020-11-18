package com.lsmt.log

import com.lsmt.core.checksum
import com.lsmt.counting
import com.lsmt.toInt
import mu.KotlinLogging
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.CRC32C

class BinaryLogReader<T>(
    private val filePath: Path,
    private inline val decoder: (InputStream) -> T
) {

    private val crc = CRC32C()

    fun readAll(): List<T> = read().toList()

    fun read(): Sequence<T> = sequence {
        Files.newInputStream(filePath).counting().use {
            try {
                while (it.available() > 0) {
                    val record = it.readRecord()
                    yield(record)
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
    }

    /**
     * The writer will only start a new record if there are enough bytes in the current block to write the whole header.
     * Otherwise, it will write a trailer consisting of 0s, starting the next header at the beginning of a new block.
     * Therefore, if there are fewer than 9 bytes remaining, we skip the remaining bytes in this block.
     */
    private fun CountingInputStream.readTrailer() {
        val remainingBytes = BinaryLogWriter.BLOCK_SIZE - (bytesRead % BinaryLogWriter.BLOCK_SIZE)
        if (remainingBytes < 9) {
            readNBytes(remainingBytes)
        }
    }

    private fun CountingInputStream.readHeader(): Header {
        readTrailer()
        val crc = readNBytes(4).toInt()
        val length = readNBytes(4).toInt()
        val type = read()

        return Header(crc, length, type)
    }

    private fun CountingInputStream.readData(header: Header): ByteArray {
        val data = readNBytes(header.length)
        if (crc.checksum(header.type, data) != header.crc) {
            logger.error("Corrupt record file=${filePath.fileName}")
        }
        return data
    }

    private fun CountingInputStream.readRecord(): T {
        var header = readHeader()
        val data = readData(header)

        if (header.type == BinaryLogWriter.FULL) {
            return decoder(data.inputStream())
        }

        val allData = mutableListOf(data)
        // It was a first record
        do {
            header = readHeader()
            allData.add(readData(header))
        } while (header.type != BinaryLogWriter.LAST)

        return decoder(ArraysInputStream(allData))
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }

}
