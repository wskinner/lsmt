package com.lsmt.core

import com.lsmt.table.TableKey
import com.lsmt.table.TableKeyComparator
import java.nio.file.Path
import java.util.*

typealias Compactor = Runnable
typealias Record = ByteArray
typealias Entry = Pair<String, Record?>
typealias LevelIndex = SortedMap<Int, Level>
typealias NumberedFile = Pair<Long, Path>
typealias BlockIndex = TreeMap<String, BlockHandle>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>

class TableEntry(val entry: Entry, val tableId: Long) : Comparable<TableEntry> {
    override fun compareTo(other: TableEntry): Int = TableKeyComparator.instance.compare(
        TableKey(entry.first, tableId),
        TableKey(other.entry.first, other.tableId)
    )
}

data class BlockHandle(
    val offset: Int,
    val length: Int
)
