package log

import Config
import core.Entry
import core.Record
import counting
import log.BinaryWriteAheadLogWriter.Companion.BLOCK_SIZE
import log.BinaryWriteAheadLogWriter.Companion.FULL
import log.BinaryWriteAheadLogWriter.Companion.LAST
import mu.KotlinLogging
import toByteArray
import toInt
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.CRC32C
import kotlin.math.min

interface WriteAheadLogManager : WriteAheadLogWriter {
    // Start a new log file. Return the path of the old log file.
    fun rotate(): Path

    // Read the current log file and return its contents merged and sorted.
    fun read(): List<Entry>
}

interface WriteAheadLogWriter : AutoCloseable {

    /**
     * Append a record to the log. Return the number of bytes written.
     * Passing a null record signifies a deletion.
     */
    fun append(key: String, value: Record?): Int

    // Total number of bytes written to the file.
    fun size(): Int
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
    private val filePrefix: String = Config.walPrefix,
    private val fileGenerator: FileGenerator = SynchronizedFileGenerator(rootDirectory, filePrefix)
) : WriteAheadLogManager {
    var id: Int
        private set

    var filePath: Path
        private set

    init {
        val numberedFile = fileGenerator.next()
        id = numberedFile.first
        filePath = numberedFile.second
    }

    private var writer: WriteAheadLogWriter = createWriter(filePath)

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
        logger.info("Shutting down WAL")
        writer.close()
    }

    override fun append(key: String, value: Record?): Int = writer.append(key, value)

    override fun size(): Int = writer.size()

    override fun rotate(): Path {
        val oldPath = filePath
        close()
        val numberedFile = fileGenerator.next()
        id = numberedFile.first
        filePath = numberedFile.second
        writer = createWriter(filePath)
        return oldPath
    }

    override fun read(): List<Entry> = BinaryWriteAheadLogReader(filePath) { it.decode() }.read()

    private fun createWriter(path: Path): WriteAheadLogWriter {
        val os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
            .buffered(BLOCK_SIZE)
        return BinaryWriteAheadLogWriter(os)
    }


    companion object {
        val logger = KotlinLogging.logger { }
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

    override fun append(key: String, value: Record?): Int {
        // A header is always 9 bytes.
        val data = encode(key, value)
        return appendBytes(data)
    }

    fun appendBytes(data: ByteArray): Int {
        val startingBytes = totalBytes
        var length: Int
        var check: Int
        var type: Int
        var offset = 0

        length = data.size
        var remainingBytes = writeTrailer()
        if (length <= remainingBytes + 9) {
            type = FULL
            check = crc.checksum(type, data)
            write(check, length, type, data, offset)
        } else {
            length = remainingBytes
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

    override fun close() {
        os.flush()
        os.close()
    }

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
            repeat((1..remainingBytes).count()) {
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

class BinaryWriteAheadLogReader<T>(
    private val filePath: Path,
    private val decoder: (InputStream) -> T
) {

    private val crc = CRC32C()

    fun read(): List<T> {
        val result = mutableListOf<T>()
        logger.info { "read() file=${filePath.fileName}" }
        Files.newInputStream(filePath).counting().use {
            try {
                while (it.available() > 0) {
                    val record = it.readRecord()
                    result.add(record)
                }
            } catch (e: Throwable) {
                e.printStackTrace()
            }
        }
        return result
    }

    /**
     * The writer will only start a new record if there are enough bytes in the current block to write the whole header.
     * Otherwise, it will write a trailer consisting of 0s, starting the next header at the beginning of a new block.
     * Therefore, if there are fewer than 9 bytes remaining, we skip the remaining bytes in this block.
     */
    private fun CountingInputStream.readTrailer() {
        val remainingBytes = BLOCK_SIZE - (bytesRead % BLOCK_SIZE)
        if (remainingBytes < 9) {
            readNBytes(remainingBytes.toInt())
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

        if (header.type == FULL) {
            return decoder(data.inputStream())
        }

        val allData = mutableListOf(data)
        // It was a first record
        do {
            header = readHeader()
            allData.add(readData(header))
        } while (header.type != LAST)

        return decoder(ArraysInputStream(allData))
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }

}

fun createLogReader(path: Path): BinaryWriteAheadLogReader<Entry> = BinaryWriteAheadLogReader(path) { it.decode() }

private fun CRC32C.checksum(type: Int, data: ByteArray, offset: Int = 0, length: Int = data.size): Int {
    reset()
    update(type)
    update(data, offset, length)
    return value.toInt()
}
