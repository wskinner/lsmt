package com.lsmt.table

data class TableKey(val minKey: String, val id: Long)

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
        val strCmp = o1!!.minKey.compareTo(o2!!.minKey)

        if (strCmp == 0) {
            return o1.id.compareTo(o2.id)
        }

        return strCmp
    }
}
