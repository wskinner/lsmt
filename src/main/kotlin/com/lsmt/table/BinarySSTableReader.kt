package com.lsmt.table

import com.lsmt.Config
import com.lsmt.core.Record
import com.lsmt.core.makeFile
import com.lsmt.log.createLogReader
import java.io.File
import java.util.*

class BinarySSTableReader(
    private val rootDirectory: File,
    private val prefix: String = Config.sstablePrefix
) : SSTableReader {
    override fun read(table: SSTableMetadata, key: String): Record? {
        val file = makeFile(rootDirectory, prefix, table.id)
        val reader = createLogReader(file)

        return reader.readAll()
            .firstOrNull { it.first == key }
            ?.second
    }

    override fun readAll(table: SSTableMetadata): SortedMap<String, Record?> {
        val file = makeFile(rootDirectory, prefix, table.id)
        val reader = createLogReader(file)
        val entries = reader.readAll()
        val result = TreeMap<String, Record?>()
        for (entry in entries) {
            result[entry.first] = entry.second
        }

        return result
    }
}
