package table

class TableKey(val minKey: String, val id: Int) : Comparable<TableKey> {
    override fun compareTo(other: TableKey): Int {
        val strCmp = minKey.compareTo(minKey)

        if (strCmp == 0) {
            return id.compareTo(other.id)
        }

        return strCmp
    }

    override fun equals(other: Any?): Boolean = when (other) {
        is TableKey -> other.minKey == minKey && other.id == id
        else -> false
    }

    override fun hashCode(): Int = 31 * minKey.hashCode() + id
}
