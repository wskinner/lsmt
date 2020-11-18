package com.lsmt.table

import com.lsmt.domain.Level
import com.lsmt.domain.LevelIndex
import com.lsmt.log.BinaryLogReader
import com.lsmt.toSSTableMetadata
import java.util.*

/**
 * TODO Instead of implementing deletes at the manifest layer, use the delete functionality that is now build into the
 * log layer.
 */
class BinaryManifestReader(
    private val logReader: BinaryLogReader
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
