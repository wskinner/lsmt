import core.KeyRange
import core.Record
import core.max
import core.min
import log.CountingInputStream
import table.SSTableMetadata
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

fun Record.toSSTableMetadata(): SSTableMetadata? {
    return SSTableMetadata(
        (this["name"] ?: return null) as String,
        (this["minKey"] ?: return null) as String,
        (this["maxKey"] ?: return null) as String,
        (this["level"] ?: return null) as Int,
        (this["id"] ?: return null) as Int,
        (this["fileSize"] ?: return null) as Int
    )
}

infix fun KeyRange.overlaps(other: KeyRange): Boolean = other.contains(start) || contains(other.start)

fun KeyRange.merge(other: KeyRange): KeyRange = KeyRange(min(start, other.start), max(endInclusive, other.endInclusive))
