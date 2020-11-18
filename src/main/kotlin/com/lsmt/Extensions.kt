package com.lsmt

import Bytes
import com.lsmt.core.KeyRange
import com.lsmt.core.Record
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.CountingInputStream
import com.lsmt.log.DELETE_MASK
import com.lsmt.log.Header
import com.lsmt.table.ByteBufferInputStream
import com.lsmt.table.SSTableMetadata
import com.lsmt.table.StandardTableBuffer
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer

fun Int.toByteArray(): ByteArray = Bytes.intToBytes(this)
fun Long.toByteArray(): ByteArray = Bytes.longToBytes(this)
fun Float.toByteArray(): ByteArray = Bytes.floatToBytes(this)
fun Double.toByteArray(): ByteArray = Bytes.doubleToBytes(this)

fun ByteArray.toInt(): Int = Bytes.bytesToInt(this)
fun ByteArray.toLong(): Long = Bytes.bytesToLong(this)
fun ByteArray.toFloat(): Float = Bytes.bytesToFloat(this)
fun ByteArray.toDouble(): Double = Bytes.bytesToDouble(this)

fun InputStream.counting() = CountingInputStream(this)
fun InputStream.readInt() = readNBytes(4).toInt()
fun InputStream.readLong() = readNBytes(8).toLong()
fun InputStream.readString(len: Int) = readNBytes(len).decodeToString()

fun ByteBuffer.inputStream() = ByteBufferInputStream(this)

fun Record.toSSTableMetadata(): SSTableMetadata? {
    val stream = inputStream()
    val pathLength = stream.readInt()
    val path = stream.readString(pathLength)
    val minKeyLength = stream.readInt()
    val minKey = stream.readString(minKeyLength)
    val maxKeyLength = stream.readInt()
    val maxKey = stream.readString(maxKeyLength)
    val level = stream.readInt()
    val id = stream.readLong()
    val fileSize = stream.readInt()

    return SSTableMetadata(
        path,
        minKey,
        maxKey,
        level,
        id,
        fileSize
    )
}

infix fun KeyRange.overlaps(other: KeyRange): Boolean = other.contains(start) || contains(other.start)

fun MappedByteBuffer.tableBuffer(table: SSTableMetadata) = StandardTableBuffer(this, table)

fun ByteBuffer.readInt(): Int = readNBytes(4).toInt()

fun ByteBuffer.readString(length: Int): String = readNBytes(length).decodeToString()

fun ByteBuffer.readHeader(): Header {
    readTrailer()
    val crc = readInt()
    val length = readInt()
    val type = get().toInt()
    return Header(crc, length, type)
}

fun ByteBuffer.readNBytes(length: Int): ByteArray {
    val result = ByteArray(length)
    get(result)
    return result
}

/**
 * The writer will only start a new record if there are enough bytes in the current block to write the whole header.
 * Otherwise, it will write a trailer consisting of 0s, starting the next header at the beginning of a new block.
 * Therefore, if there are fewer than 9 bytes remaining, we skip the remaining bytes in this block.
 */
fun ByteBuffer.readTrailer() {
    var remainingBytes = BinaryLogWriter.BLOCK_SIZE - (position() % BinaryLogWriter.BLOCK_SIZE)
    if (remainingBytes < 9) {
        while (remainingBytes > 0) {
            get()
            remainingBytes--
        }
    }
}

data class Key(val value: String, val isDelete: Boolean, val remainingBytes: Int)

/**
 * Before calling this function, the position should be set to the first byte of a FIRST or FULL header.
 *
 * Returns the key and the number of bytes left to be read in the current record - either zero or a positive number.
 * // TODO (will) add CRC check
 */
fun ByteBuffer.readKey(firstHeader: Header): Key? {
    var header = firstHeader
    val sizeArr = ByteArray(4)
    var bytesRead = 0
    if (header.length < 4) {
        for (i in 0 until header.length)
            sizeArr[i] = get()
        bytesRead = header.length
        header = readHeader()
    }

    for (i in bytesRead until 4)
        sizeArr[i] = get()

    // The number of bytes left to read in the current record
    val remainingBytes = header.length - 4 + bytesRead
    val size = sizeArr.toInt()
    val isDelete = size and DELETE_MASK < 0
    val keySize = size and Integer.MAX_VALUE

    val keyBytes = ByteArray(keySize)
    var pos = 0
    if (keySize < remainingBytes) {
        val key = readString(keySize)
        return Key(key, isDelete, remainingBytes - keySize)
    }

    System.arraycopy(
        readNBytes(remainingBytes),
        0,
        keyBytes,
        pos,
        remainingBytes
    )
    pos += remainingBytes

    while (pos < keySize) {
        readTrailer()
        header = readHeader()
        if (header.length < keySize - pos) {
            System.arraycopy(
                readNBytes(header.length),
                0,
                keyBytes,
                pos,
                header.length
            )
            pos += header.length
        } else {
            System.arraycopy(
                readNBytes(keySize - pos),
                0,
                keyBytes,
                pos,
                keySize - pos
            )
            return Key(keyBytes.decodeToString(), isDelete, header.length - (keySize - pos))
        }
    }

    return null
}
