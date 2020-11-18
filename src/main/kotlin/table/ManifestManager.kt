package table

import core.*
import log.BinaryWriteAheadLogReader
import log.WriteAheadLogWriter
import table.StandardManifestManager.Companion.add
import table.StandardManifestManager.Companion.remove
import toSSTableMetadata
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
interface ManifestManager {
    // Map from level number to Level
    fun levels(): LevelIndex

    fun level(level: Int): Level = levels().getOrDefault(level, emptyLevel())

    fun getOrPut(index: Int): Level

    fun addTable(table: SSTableMetadata)

    fun removeTable(table: SSTableMetadata)
}

interface ManifestWriter {
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

    fun toRecord(): SortedMap<String, Any> {
        val map = TreeMap<String, Any>()
        map["path"] = path
        map["minKey"] = minKey
        map["maxKey"] = maxKey
        map["level"] = level
        map["id"] = id
        return map
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
    private val levelFactory: () -> Level
) : ManifestManager {
    private val allTables: LevelIndex = reader.read()

    override fun levels(): LevelIndex = allTables

    override fun getOrPut(index: Int): Level = synchronized(this) {
        if (!allTables.containsKey(index)) {
            allTables[index] = levelFactory()
        }
        return level(index)
    }

    override fun addTable(table: SSTableMetadata): Unit {
        writer.addTable(table)

        synchronized(this) {
            if (!allTables.containsKey(table.level)) {
                allTables[table.level] = levelFactory()
            }

            allTables[table.level]?.add(table)
        }
    }

    override fun removeTable(table: SSTableMetadata): Unit = synchronized(this) {
        allTables[table.level]?.remove(table)
    }

    companion object {
        const val add = "1"
        const val remove = "2"
    }
}

class BinaryManifestReader(
    private val logReader: BinaryWriteAheadLogReader<Entry>
) : ManifestReader {
    override fun read(): LevelIndex {
        val result = TreeMap<Int, Level>()

        logReader.read().forEach { (type, record) ->
            val tableMeta = record.toSSTableMetadata()!!
            when (type) {
                add -> result[tableMeta.level]?.add(tableMeta)
                remove -> result[tableMeta.level]?.remove(tableMeta)
            }
        }
        return result
    }
}

class BinaryManifestWriter(
    private val logWriter: WriteAheadLogWriter
) : ManifestWriter {

    override fun addTable(table: SSTableMetadata) {
        logWriter.append(add, table.toRecord())
    }

    override fun removeTable(table: SSTableMetadata) {
        logWriter.append(remove, table.toRecord())
    }

}
