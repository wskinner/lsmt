package table

import log.WriteAheadLogWriter

interface TableMerger {

    /**
     * Given a collection of tables which span a certain range, split the tables into one or new tables, which
     * together span the same range, such that no table is larger than the maximum number of bytes.
     */
    fun merge(tables: Collection<SSTableMetadata>, maxBytes: Int): List<SSTableMetadata>
}

class StandardTableMerger(
    private val writer: WriteAheadLogWriter
) : TableMerger {
    override fun merge(tables: Collection<SSTableMetadata>, maxBytes: Int): List<SSTableMetadata> {

    }
}
