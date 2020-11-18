package com.lsmt.table

import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.tableBuffer
import java.io.RandomAccessFile
import java.nio.channels.FileChannel.MapMode.READ_ONLY
import java.util.*

/**
 * Handles logic for reading SSTables.
 */
interface SSTableReader {
    fun readAll(table: SSTableMetadata): SortedMap<Key, Record?>

    /**
     * Memory map the file and return the resulting SSTable.
     */
    fun mmap(table: SSTableMetadata): SSTable
}

class BinarySSTableReader : SSTableReader {
    override fun readAll(table: SSTableMetadata): SortedMap<Key, Record?> {
        val reader = mmap(table).iterator()
        val result = TreeMap<Key, Record?>()
        for (entry in reader) {
            result[entry.first] = entry.second
        }

        return result
    }

    override fun mmap(table: SSTableMetadata): SSTable = StandardSSTable(
        RandomAccessFile(table.path, "r")
            .channel
            .map(READ_ONLY, 0, table.fileSize.toLong())
            .tableBuffer(table)
    )
}
