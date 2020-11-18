package com.lsmt.table

import com.lsmt.log.LogWriter

class BinaryManifestWriter(
    private val logWriter: LogWriter
) : ManifestWriter {

    override fun addTable(table: SSTableMetadata) {
        logWriter.append(StandardManifestManager.add, table.toRecord())
    }

    override fun removeTable(table: SSTableMetadata) {
        logWriter.append(StandardManifestManager.remove, table.toRecord())
    }

    override fun close() {
        logWriter.close()
    }

}
