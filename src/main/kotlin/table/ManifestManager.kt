package table

import core.KeyRange
import core.TableIndex
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
    // Map from level to tables
    fun tables(): TableIndex

    fun addTable(table: SSTableMetadata)

    fun removeTable(table: SSTableMetadata)
}

interface ManifestWriter {
    fun addTable(table: SSTableMetadata)
    fun removeTable(table: SSTableMetadata)
}

interface ManifestReader {
    fun read(): TableIndex
}

data class SSTableMetadata(
    val name: String,
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
        map["name"] = name
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
    reader: ManifestReader
) : ManifestManager {
    private val allTables: TableIndex = reader.read()

    override fun tables(): TableIndex = allTables

    override fun addTable(table: SSTableMetadata) {
        writer.addTable(table)

        if (!allTables.containsKey(table.level)) {
            allTables[table.level] = TreeMap()
        }

        allTables[table.level]?.put(table.key, table)
    }

    override fun removeTable(table: SSTableMetadata) {
        allTables[table.level]?.remove(table.key)
    }

    companion object {
        const val add = "1"
        const val remove = "2"
    }
}

class BinaryManifestReader(
    private val logReader: BinaryWriteAheadLogReader
) : ManifestReader {
    override fun read(): TableIndex {
        val result = TreeMap<Int, SortedMap<TableKey, SSTableMetadata>>()

        logReader.read().forEach { (type, record) ->
            val tableMeta = record.toSSTableMetadata()
            when (type) {
                add -> result[tableMeta?.level]?.put(tableMeta?.key, tableMeta)
                remove -> result[tableMeta?.level]?.get(tableMeta?.key)
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
