package log

import core.Entry
import core.Record
import core.currentFile
import core.nextFile
import counting
import log.BinaryWriteAheadLogWriter.Companion.BLOCK_SIZE
import log.BinaryWriteAheadLogWriter.Companion.FULL
import log.BinaryWriteAheadLogWriter.Companion.LAST
import toByteArray
import toInt
import java.io.File
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32C
import kotlin.math.min

interface WriteAheadLogManager : WriteAheadLogWriter {
    // Return the size, in bytes, of the log file.
    fun size(): Long

    // Start a new log file. Return the path of the old log file.
    fun rotate(): Path

    // Read the current log file and return its contents merged and sorted.
    fun read(): List<Entry>
}

interface WriteAheadLogWriter : AutoCloseable {

    // Append a record to the log. Return the number of bytes written.
    fun append(key: String, value: Record): Int
}

data class Header(val crc: Int, val length: Int, val type: Int)

/**
 *
 * The log file contents are a sequence of 32KB blocks. The only exception is that the tail of the file may contain a partial block.
 *
 * block := record* trailer?
 * record :=
 * checksum: int32     // crc32c of type and data[] ; little-endian
 * length: int32       // little-endian
 * type: uint8          // One of FULL, FIRST, MIDDLE, LAST
 * data: uint8[length]
 */
class BinaryWriteAheadLogManager(
    private val rootDirectory: File,
    private val filePrefix: String = "wal_"
) : WriteAheadLogManager {
    private var writer: WriteAheadLogWriter = createWriter()
    private var filePath = Paths.get("$filePrefix${currentFile(rootDirectory, filePrefix)}")

    /**
     * Not thread safe.
     * 1. Convert the key and value to one or more records. Depending on the size of the data and how many bytes remain
     *    in the current block, the number of records will vary.
     * 2. Write the record(s) to the file
     *
     * The record header (checksum, length, type) requires 9 bytes. If the current block has less than 9 bytes remaining,
     * the remaining bytes must be zeroes.
     */
    override fun close() {
        writer.close()
    }

    override fun append(key: String, value: Record): Int = writer.append(key, value)

    override fun size(): Long {
        return Files.size(filePath)
    }

    override fun rotate(): Path {
        val nextIndex = nextFile(
            filePath.parent.toFile(),
            filePrefix
        )
        val newPath = Paths.get("$filePrefix$nextIndex")
        filePath = newPath
        close()
        Files.move(filePath, newPath)
        writer = createWriter()
        return newPath
    }

    override fun read(): List<Entry> {
        return BinaryWriteAheadLogReader(filePath).read()
    }

    private fun createWriter(): WriteAheadLogWriter {
        val os = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        return BinaryWriteAheadLogWriter(os)
    }
}

/**
 * Implements a binary protocol based on the one used by LevelDB.
 */
class BinaryWriteAheadLogWriter(
    private val os: OutputStream
) : WriteAheadLogWriter {
    private val crc = CRC32C()
    private var totalBytes: Int = 0

    override fun append(key: String, value: Record): Int {
        // A header is always 9 bytes.
        val data = encode(key, value)
        var length: Int
        var check: Int
        var type: Int
        var offset = 0

        length = data.size
        var remainingBytes = writeTrailer()

        var bytesWritten = 0
        if (length <= remainingBytes + 9) {
            type = FULL
            check = crc.checksum(type, data)
            bytesWritten += write(check, length, type, data, offset)
        } else {
            length = remainingBytes
            type = FIRST
            check = crc.checksum(type, data, offset, length)
            bytesWritten += write(check, length, type, data, offset)

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
                bytesWritten += write(check, length, type, data, offset)
            } while (data.size > offset + length)
        }
        return bytesWritten
    }

    override fun close() {
        os.close()
    }

    private fun write(check: Int, length: Int, type: Int, data: ByteArray, offset: Int): Int {
        val checkBytes = check.toByteArray()
        val lengthBytes = length.toByteArray()
        os.write(checkBytes)
        os.write(lengthBytes)
        os.write(type)
        os.write(data, offset, length)
        val bytes = checkBytes.size + lengthBytes.size + 1 + data.size
        totalBytes += bytes
        return bytes
    }

    /**
     * If needed, write whitespace to fill the end of the block. Return the number of bytes remaining in the current
     * block.
     */
    private fun writeTrailer(): Int {
        val remainingBytes =
            BLOCK_SIZE - totalBytes % BLOCK_SIZE
        if (remainingBytes < 9) {
            repeat((1..remainingBytes).count()) {
                os.write(0)
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

class BinaryWriteAheadLogReader(
    private val filePath: Path
) {

    private val crc = CRC32C()

    fun read(): List<Entry> {
        val result = mutableListOf<Entry>()
        filePath.toFile().inputStream().counting().use {
            while (it.available() > 0) {
                val record = it.readRecord()
                result.add(record)
            }
        }

        return result.sortedBy { it.first }
    }

    private fun CountingInputStream.readHeader(): Header {
        val crc = readNBytes(4).toInt()
        val length = readNBytes(4).toInt()
        val type = read()

        return Header(crc, length, type)
    }

    private fun CountingInputStream.readData(header: Header): ByteArray {
        val data = readNBytes(header.length)
        if (crc.checksum(header.type, data) != header.crc) {
            println("Corrupt record")
        }
        return data
    }

    private fun CountingInputStream.readRecord(): Entry {
        var header = readHeader()
        val data = readData(header)

        if (header.type == FULL) {
            return data.inputStream().decode()
        }

        val allData = mutableListOf(data)
        // It was a first record
        do {
            header = readHeader()
            allData.add(readData(header))

            val bytesRemainingInBlock =
                (BLOCK_SIZE - (bytesRead % BLOCK_SIZE)).toInt()
            if (bytesRemainingInBlock < 9) {
                // We'll never start a new record with < 9 bytes remaining, since the header will not fit in a
                // single block. The remaining bytes will have been filled with zero padding. Skip them.
                readNBytes(bytesRemainingInBlock)
            }
        } while (header.type != LAST)

        return ArraysInputStream(allData).decode()
    }

}

private fun CRC32C.checksum(type: Int, data: ByteArray, offset: Int = 0, length: Int = data.size): Int {
    reset()
    update(type)
    update(data, offset, length)
    return value.toInt()
}
