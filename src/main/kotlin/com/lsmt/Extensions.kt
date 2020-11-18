package com.lsmt

import Bytes
import com.lsmt.domain.Entry
import com.lsmt.domain.Key
import com.lsmt.domain.KeyRange
import com.lsmt.domain.Record
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.CountingInputStream
import com.lsmt.log.Header
import com.lsmt.table.ByteBufferInputStream
import com.lsmt.table.SSTableMetadata
import com.lsmt.table.StandardTableBuffer
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer

fun Int.toByteArray(littleEndian: Boolean = true): ByteArray = Bytes.intToBytes(this, littleEndian)
fun Long.toByteArray(littleEndian: Boolean = true): ByteArray = Bytes.longToBytes(this, littleEndian)
fun Float.toByteArray(littleEndian: Boolean = true): ByteArray = Bytes.floatToBytes(this, littleEndian)
fun Double.toByteArray(littleEndian: Boolean = true): ByteArray = Bytes.doubleToBytes(this, littleEndian)

fun ByteArray.toInt(littleEndian: Boolean = true): Int = Bytes.bytesToInt(this, littleEndian)
fun ByteArray.toLong(littleEndian: Boolean = true): Long = Bytes.bytesToLong(this, littleEndian)
fun ByteArray.toFloat(littleEndian: Boolean = true): Float = Bytes.bytesToFloat(this, littleEndian)
fun ByteArray.toDouble(littleEndian: Boolean = true): Double = Bytes.bytesToDouble(this, littleEndian)

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
    val minKey = stream.readNBytes(minKeyLength)
    val maxKeyLength = stream.readInt()
    val maxKey = stream.readNBytes(maxKeyLength)
    val level = stream.readInt()
    val id = stream.readLong()
    val fileSize = stream.readInt()

    return SSTableMetadata(
        path,
        minKey.toKey(),
        maxKey.toKey(),
        level,
        id,
        fileSize
    )
}

infix fun KeyRange.overlaps(other: KeyRange): Boolean = other.contains(start) || contains(other.start)

fun MappedByteBuffer.tableBuffer(table: SSTableMetadata) = StandardTableBuffer(this, table)

fun ByteBuffer.readInt(): Int = readNBytes(4).toInt()

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

/**
 * Byte arrays are compared left to right. Bytes are interpreted as unsigned 8-bit integers.
 * For arrays of the same length, compare each byte from left to right.
 * For arrays of different lengths, compare each of the first n bytes, where n is minimum length between the two arrays.
 * If the first n bytes are the same, the longer array is considered greater.
 */
operator fun UByteArray.compareTo(other: UByteArray): Int {
    val minLength = Integer.min(size, other.size)
    for (i in 0 until minLength) {
        val byteCmp = this[i].compareTo(other[i])
        if (byteCmp != 0)
            return byteCmp
    }
    return size - other.size
}

operator fun ByteArray.compareTo(other: ByteArray): Int = this.asUByteArray().compareTo(other.asUByteArray())

fun ByteArray.toKey(): Key = Key(this.asUByteArray())

fun UByteArray.toKey(): Key = Key(this)
