import core.*
import log.CountingInputStream
import log.createLogReader
import table.SSTableMetadata
import java.io.InputStream
import java.nio.file.Paths
import java.util.*

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
        (this["path"] ?: return null) as String,
        (this["minKey"] ?: return null) as String,
        (this["maxKey"] ?: return null) as String,
        (this["level"] ?: return null) as Int,
        (this["id"] ?: return null) as Int,
        (this["fileSize"] ?: return null) as Int
    )
}

infix fun KeyRange.overlaps(other: KeyRange): Boolean = other.contains(start) || contains(other.start)

fun KeyRange.merge(other: KeyRange): KeyRange = KeyRange(min(start, other.start), max(endInclusive, other.endInclusive))

fun SSTableMetadata.toSequence(): Sequence<Entry> = sequence {
    val reader = createLogReader(Paths.get(path))
    yieldAll(reader.read())
}

/**
 * Transforms a stream of Entry, which may represent an insertion or a deletion, into a stream of SafeEntry. This
 * represents a snapshot of the state of the system at the time the last Entry was appended to the stream.
 *
 * The resulting Iterable is no longer ordered by insertion time, but by key.
 */
fun Sequence<Entry>.merge(): SortedMap<String, Record> {
    val map = TreeMap<String, Record>()

    for (entry in iterator()) {
        if (entry.second == null) {
            map.remove(entry.first)
        } else {
            // The compiler can't seem to tell that entry.second is never null here
            map[entry.first] = entry.second!!
        }
    }

    return map
}

fun Iterable<Entry>.merge(): SortedMap<String, Record> = asSequence().merge()
