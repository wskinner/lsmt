package com.lsmt.core

import java.nio.file.Path
import java.util.*

typealias Compactor = Runnable
typealias Record = ByteArray
typealias Entry = Pair<String, Record?>
typealias LevelIndex = SortedMap<Int, Level>
typealias NumberedFile = Pair<Long, Path>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>

class TableEntry(val entry: Entry, val tableId: Int) : Comparable<TableEntry> {
    override fun compareTo(other: TableEntry): Int {
        val strCmp = entry.first.compareTo(other.entry.first)
        if (strCmp == 0) {
            return tableId.compareTo(other.tableId)
        }
        return strCmp
    }
}
