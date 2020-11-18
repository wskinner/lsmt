package com.lsmt.core

import com.lsmt.compareTo
import com.lsmt.table.TableKey
import com.lsmt.table.TableKeyComparator
import java.nio.file.Path
import java.util.*

typealias Compactor = Runnable
typealias Record = ByteArray
typealias Entry = Pair<Key, Record?>
typealias LevelIndex = SortedMap<Int, Level>
typealias NumberedFile = Pair<Long, Path>
typealias BlockIndex = TreeMap<Key, BlockHandle>

class KeyRange(override val start: Key, override val endInclusive: Key) : ClosedRange<Key>

class TableEntry(val entry: Entry, val tableId: Long) : Comparable<TableEntry> {
    override fun compareTo(other: TableEntry): Int = TableKeyComparator.instance.compare(
        TableKey(entry.first, tableId),
        TableKey(other.entry.first, other.tableId)
    )
}

data class BlockHandle(
    val offset: Int
)

data class Key(val byteArray: UByteArray) : Comparable<Key> {
    override fun compareTo(other: Key): Int = byteArray.compareTo(other.byteArray)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Key

        if (!byteArray.contentEquals(other.byteArray)) return false

        return true
    }

    override fun hashCode(): Int {
        return byteArray.contentHashCode()
    }

    val size = byteArray.size
}
