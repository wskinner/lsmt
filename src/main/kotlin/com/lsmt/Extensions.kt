package com.lsmt

import Bytes
import com.lsmt.core.KeyRange
import com.lsmt.core.Record
import com.lsmt.core.max
import com.lsmt.core.min
import com.lsmt.log.CountingInputStream
import com.lsmt.table.SSTableMetadata
import java.io.InputStream

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
fun InputStream.readString(len: Int) = readNBytes(len).decodeToString()

fun Record.toSSTableMetadata(): SSTableMetadata? {
    val stream = inputStream()
    val pathLength = stream.readInt()
    val path = stream.readString(pathLength)
    val minKeyLength = stream.readInt()
    val minKey = stream.readString(minKeyLength)
    val maxKeyLength = stream.readInt()
    val maxKey = stream.readString(maxKeyLength)
    val level = stream.readInt()
    val id = stream.readInt()
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

fun KeyRange.merge(other: KeyRange): KeyRange = KeyRange(min(start, other.start), max(endInclusive, other.endInclusive))

