package com.lsmt.domain

import com.lsmt.table.TableKey
import com.lsmt.table.TableKeyComparator
import java.nio.file.Path
import java.util.*

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

data class BlockHandle(val offset: Int)
