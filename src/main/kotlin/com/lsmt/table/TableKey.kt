package com.lsmt.table

import com.lsmt.domain.Key

data class TableKey(val minKey: Key, val id: Long) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TableKey

        if (minKey != other.minKey) return false
        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        var result = minKey.hashCode()
        result = 31 * result + id.hashCode()
        return result
    }
}

/**
 * It is desirable to use a Comparator here instead of the natural ordering of TableKey so that we can avoid costly NPEs
 * in {@link java.util.TreeMap#floorEntry}
 */
class TableKeyComparator : Comparator<TableKey> {
    override fun compare(o1: TableKey?, o2: TableKey?): Int {
        if (o1 == null && o2 == null)
            return 0

        if (o1 == null && o2 != null)
            return -1

        if (o1 != null && o2 == null)
            return 1

        // Neither key is null
        val bytesCmp = o1!!.minKey.compareTo(o2!!.minKey)

        if (bytesCmp == 0) {
            return o1.id.compareTo(o2.id)
        }

        return bytesCmp
    }

    companion object {
        val instance = TableKeyComparator()
    }
}
