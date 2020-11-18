package com.lsmt.table

import com.lsmt.core.*
import com.lsmt.log.BinaryLogReader
import com.lsmt.log.LogWriter
import com.lsmt.table.StandardManifestManager.Companion.add
import com.lsmt.table.StandardManifestManager.Companion.remove
import com.lsmt.toByteArray
import com.lsmt.toSSTableMetadata
import mu.KotlinLogging
import java.io.ByteArrayOutputStream
import java.util.*

/**
 * The manifest file contains the state of the levels. We need to keep track of all the SSTable files, and which level
 * they are in.
 *
 * A few types of operations can occur.
 * 1. Adding a new table to L0.
 * 2. Merging all the tables in L0 into the overlapping tables in L1.
 * 3. Merging a single table from Li (i > 0) into the overlapping tables in Li+1 (compaction).
 *
 * Each of these operations can be represented as a series of removals and additions of tables.
 */
interface ManifestManager : AutoCloseable {
    // Map from level number to Level
    fun levels(): LevelIndex

    fun level(level: Int): Level = levels().getOrDefault(level, emptyLevel())

    fun getOrPut(index: Int): Level

    fun addTable(table: SSTableMetadata)

    fun removeTable(table: SSTableMetadata)
}

interface ManifestWriter : AutoCloseable {
    fun addTable(table: SSTableMetadata)

    fun removeTable(table: SSTableMetadata)
}

interface ManifestReader {
    fun read(): LevelIndex
}

data class SSTableMetadata(
    val path: String,
    val minKey: String,
    val maxKey: String,
    val level: Int,
    val id: Int,
    val fileSize: Int,
    val keyRange: KeyRange = KeyRange(minKey, maxKey),
    val key: TableKey = TableKey(minKey, id)
) {

    /**
     * Format:
     * path length   := int32
     * path          := uint8[path length]
     * minKey length := int32
     * minKey        := uint8[minkKey length]
     * maxKey length := int32
     * maxKey        := uint8[maxKey length]
     * level         := int32
     * id            := int32
     * fileSize      := int32
     *
     */
    fun toRecord(): ByteArray {
        val os = ByteArrayOutputStream()
        os.write(path.length.toByteArray())
        os.write(path.toByteArray())
        os.write(minKey.length.toByteArray())
        os.write(minKey.toByteArray())
        os.write(maxKey.length.toByteArray())
        os.write(maxKey.toByteArray())
        os.write(level.toByteArray())
        os.write(id.toByteArray())
        os.write(fileSize.toByteArray())
        return os.toByteArray()
    }
}

/**
 * Records the set of SSTables that make up each level, their key ranges, and other metadata. A new manifest is created
 * whenever the engine restarts.
 *
 * At startup, we read the old manifest file to refresh the in-memory state, then start a new manifest file.
 *
 * The first line of the file includes the starting state. All subsequent lines represent modifications to the state.
 *
 * Periodically, rotate the manifest for faster startup next time.
 *
 * The manifest reuses the binary storage protocol of the write ahead log. Its on disk format is simply a series of
 * key-value pairs where the key is a string and the value is a sorted map.
 */
class StandardManifestManager(
    private val writer: ManifestWriter,
    reader: ManifestReader,
    private val levelFactory: (Int) -> Level
) : ManifestManager {
    private val allTables: LevelIndex = reader.read()

    override fun levels(): LevelIndex = allTables

    override fun getOrPut(index: Int): Level = synchronized(this) {
        if (!allTables.containsKey(index)) {
            allTables[index] = levelFactory(index)
        }
        return level(index)
    }

    override fun addTable(table: SSTableMetadata) {
        logger.debug { "addTable() level=${table.level} id=${table.id}" }
        writer.addTable(table)

        synchronized(this) {
            if (!allTables.containsKey(table.level)) {
                allTables[table.level] = levelFactory(table.level)
            }

            allTables[table.level]?.add(table)
        }
    }

    override fun removeTable(table: SSTableMetadata): Unit = synchronized(this) {
        logger.debug { "removeTable() level=${table.level} id=${table.id}" }
        try {
            allTables[table.level]?.remove(table)
        } catch (t: Throwable) {
            logger.error(t) { "Error in removeTable()" }
        }
    }

    override fun close() {
        writer.close()
    }

    companion object {
        val logger = KotlinLogging.logger {}
        const val add = "1"
        const val remove = "2"
    }
}

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
                add -> result[tableMeta.level]?.add(tableMeta)
                remove -> result[tableMeta.level]?.remove(tableMeta)
            }
        }
        return result
    }
}

class BinaryManifestWriter(
    private val logWriter: LogWriter
) : ManifestWriter {

    override fun addTable(table: SSTableMetadata) {
        logWriter.append(add, table.toRecord())
    }

    override fun removeTable(table: SSTableMetadata) {
        logWriter.append(remove, table.toRecord())
    }

    override fun close() {
        logWriter.close()
    }

}
