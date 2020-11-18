package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Level
import com.lsmt.core.LevelIndex
import com.lsmt.log.BinaryLogReader
import com.lsmt.toSSTableMetadata
import java.util.*

/**
 * TODO Instead of implementing deletes at the manifest layer, use the delete functionality that is now build into the
 * log layer.
 */
class BinaryManifestReader(
    private val logReader: BinaryLogReader<Entry>
) : ManifestReader {
    override fun read(): LevelIndex {
        val result = TreeMap<Int, Level>()

        logReader.readAll().forEach { (type, record) ->
            val tableMeta = record!!.toSSTableMetadata()!!
            when (type) {
                StandardManifestManager.add -> result[tableMeta.level]?.add(tableMeta)
                StandardManifestManager.remove -> result[tableMeta.level]?.remove(tableMeta)
            }
        }
        return result
    }
}