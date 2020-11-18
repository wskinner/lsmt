package log

import MemTable
import WriteAheadLogManager
import toByteArray
import toInt
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.zip.CRC32C
import kotlin.math.min

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
    private val filePath: Path
) : WriteAheadLogManager {
    private lateinit var os: OutputStream

    private val crC32C = CRC32C()

    /**
     * Not thread safe.
     * 1. Convert the key and value to one or more records. Depending on the size of the data and how many bytes remain
     *    in the current block, the number of records will vary.
     * 2. Write the record(s) to the file
     *
     * The record header (checksum, length, type) requires 9 bytes. If the current block has less than 9 bytes remaining,
     * the remaining bytes must be zeroes.
     */
    override fun append(key: String, value: SortedMap<String, Any>): Long {
        // A header is always 9 bytes.

        val data = encode(key, value)
        var length: Int
        var check: Int
        var type: Int
        var offset = 0

        length = data.size
        var remainingBytes = writeTrailer()

        if (length <= remainingBytes + 9) {
            type = full
            check = checksum(type, data)
            write(check, length, type, data, offset)
            return 0L
        } else {
            length = remainingBytes
            type = first
            check = checksum(type, data, offset, length)
            write(check, length, type, data, offset)

            do {
                remainingBytes = writeTrailer()

                // Here, length is the number of bytes written in the last write.
                offset += length

                // We'll either write all the remaining bytes, or write to the end of the current block.
                length = min(remainingBytes - 9, data.size - offset)

                type = if (data.size > offset + length) {
                    middle
                } else {
                    last
                }

                check = checksum(type, data, offset, length)
                write(check, length, type, data, offset)
            } while (data.size > offset + length)
        }

        return 0L
    }

    private fun write(check: Int, length: Int, type: Int, data: ByteArray, offset: Int) {
        println("Writing data:\n\tcheck: $check\n\tlength: $length\n\ttype: $type\n\tdata: $data\n\toffset: $offset")
        os.write(check.toByteArray())
        os.write(length.toByteArray())
        os.write(type)
        os.write(data, offset, length)
    }

    /**
     * If needed, write whitespace to fill the end of the block. Return the number of bytes remaining in the current
     * block.
     */
    private fun writeTrailer(): Int {
        val remainingBytes = blockSize - Files.size(filePath) % blockSize
        while (remainingBytes < 9) {
            os.write(0)
        }
        return blockSize - (Files.size(filePath) % blockSize).toInt()
    }

    override fun restore(): MemTable {
        TODO("Not yet implemented")
    }

    override fun close() {
        os.close()
    }

    override fun start() {
        os = Files.newOutputStream(filePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    }

    private fun checksum(type: Int, data: ByteArray, offset: Int = 0, length: Int = data.size): Int {
        crC32C.reset()
        crC32C.update(type)
        crC32C.update(data, offset, length)
        return crC32C.value.toInt()
    }

    companion object {
        const val blockSize = 1 shl 15
        const val full = 1
        const val first = 2
        const val middle = 3
        const val last = 4
    }

    data class Header(val crc: Int, val length: Int, val type: Int)

    fun read(): List<Pair<String, SortedMap<String, Any>>> {
        val result = mutableListOf<Pair<String, SortedMap<String, Any>>>()
        filePath.toFile().inputStream().counting().use {
            while (it.available() > 0) {
                val record = it.readRecord()
                result.add(record)

            }
        }

        return result
    }

    private fun CountingInputStream.readHeader(): Header {
        val crc = readNBytes(4).toInt()
        val length = readNBytes(4).toInt()
        val type = read()

        return Header(crc, length, type)
    }

    private fun CountingInputStream.readData(header: Header): ByteArray {
        val data = readNBytes(header.length)
        if (checksum(header.type, data) != header.crc) {
            println("Corrupt record")
        }
        return data
    }

    fun CountingInputStream.readRecord(): Pair<String, SortedMap<String, Any>> {
        var header = readHeader()
        println("Reading record: type=${header.type}, length=${header.length}")
        var data = readData(header)

        if (header.type == full) {
            return data.inputStream().decode()
        }

        val allData = mutableListOf(data)
        // It was a first record
        do {
            header = readHeader()
            allData.add(readData(header))

            val bytesRemainingInBlock = (blockSize - (bytesRead % blockSize)).toInt()
            if (bytesRemainingInBlock < 9) {
                // We'll never start a new record with < 9 bytes remaining, since the header will not fit in a
                // single block. The remaining bytes will have been filled with zero padding. Skip them.
                readNBytes(bytesRemainingInBlock)
            }
        } while (header.type != last)

        return ArraysInputStream(allData).decode()
    }
}
