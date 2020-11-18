package table

import Config
import core.Entry
import core.KeyRange
import core.Record
import core.nextFile
import log.BinaryWriteAheadLogReader
import log.BinaryWriteAheadLogWriter
import merge
import overlaps
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Responsible for creating and deleting SSTable files and performing compactions.
 */
interface SSTableManager : AutoCloseable {

    // Get a value from the on-disk storage.
    fun get(key: String): Record?

    // Add a new SSTable.
    fun addTable(wal: Path, level: Int = 0)

    // Perform compaction on the existing tables.
    fun doCompaction()

    fun addTableAsync(wal: Path) {

    }
}

/**
 * Handles logic for reading SSTables.
 */
interface SSTableReader {
    fun read(table: SSTableMetadata, key: String): Record?
}

/**
 * SSTable files are arranged in levels. Tables in level zero, the "young" level can have overlapping keys. Tables in
 * other levels must contain no overlapping keys. I.e. for levels greater than zero, a key will be within the key range
 * of at most one table. Since records gradually move from the lower to the higher levels, this implies that for a given
 * key, the record in the lowest level must be the most recent (except for the records in the young level, where overlap
 * may occur).
 */
class StandardSSTableManager(
    // Directory where the SSTable files will be stored
    private val rootDirectory: File,
    private val manifest: ManifestManager,
    private val tableReader: SSTableReader,
    private val config: Config
) : SSTableManager {
    private val threadPool: ExecutorService =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2)

    init {
        if (!rootDirectory.exists()) {
            rootDirectory.mkdir()
        }
    }

    override fun get(key: String): Record? = getYoung(key) ?: getOld(key)

    override fun addTable(wal: Path, level: Int) = synchronized(this) {
        // 1. Create the new table file.
        val id = nextTableId()
        val file = tableFile(rootDirectory, config.sstablePrefix, id)

        // 2. Read the wal file, merge and sort its contents, and write the result to the new table file.
        val writer = BinaryWriteAheadLogWriter(
            file.toFile()
                .outputStream()
                .buffered(Files.size(wal).toInt())
        )
        val data = BinaryWriteAheadLogReader(wal).read()
        var totalBytes = 0
        data.forEach {
            totalBytes += writer.append(it.first, it.second)
        }

        // 3. Add the new table file to the young level.
        manifest.addTable(
            SSTableMetadata(
                name = file.toString(),
                minKey = data.first().first,
                maxKey = data.last().first,
                level = level,
                id = id,
                fileSize = totalBytes
            )
        )

        // When the size of the young level exceeds a threshold, merge all young level files into all overlapping files
        // in level 1.
        if (manifest.tables()[0]?.size ?: 0 > 4) {
            mergeYoung()
        }
    }

    /**
     * Merges the young level into level 1. The result is a new level 1, in which the level i > 0 invariant is
     * maintained: no two tables in the same level may have overlapping key ranges. Since SSTables are immutable,
     * any table merges that occur will result in the creation of new tables.
     *
     * 1. Merge the levels, possibly resulting in some tables that are larger than the 2MB file limit.
     * 2. Ensure all tables are less than 2MB by splitting larger tables.
     */
    private fun mergeYoung() {
        val newLevel1 = TreeMap(manifest.tables()[0])
        manifest.tables()[1]?.let { newLevel1.putAll(it) }
        val mergeGroups = mutableListOf<List<SSTableMetadata>>()


        var currentGroup = mutableListOf(newLevel1.firstEntry().value!!)
        var currentRange = currentGroup.last().keyRange
        for (next in newLevel1.values.drop(1)) {
            if (currentRange overlaps next.keyRange) {
                currentGroup.add(next)
                currentRange = currentRange.merge(next.keyRange)
            } else {
                if (currentGroup.isNotEmpty()) {
                    mergeGroups.add(currentGroup)
                    currentGroup = mutableListOf()
                    currentRange = KeyRange("", "")
                }
            }
        }

        for (group in mergeGroups) {
            for (table in group) {
                manifest.removeTable(table)
            }

            for (table in merge(group, config.maxSstableSize)) {
                manifest.addTable(table)
            }
        }
    }

    private fun concat(tables: Collection<SSTableMetadata>): Sequence<Entry> = sequence {
        for (table in tables) {
            val reader = BinaryWriteAheadLogReader(table.tableFile())
            yieldAll(reader.read())
        }
    }

    /**
     * Given a collection of tables which span a certain range, split the tables into one or new tables, which
     * together span the same range, such that no table is larger than the maximum number of bytes.
     */
    fun merge(tables: Collection<SSTableMetadata>, maxBytes: Int): List<SSTableMetadata> {
        val result = mutableListOf<SSTableMetadata>()
        val seq = concat(tables)

        var id = nextTableId()
        var currentFile = tableFile(rootDirectory, config.sstablePrefix, id)
        var currentWriter = BinaryWriteAheadLogWriter(
            currentFile.toFile()
                .outputStream()
                .buffered(config.maxSstableSize)
        )
        var totalBytes = 0
        var minKey: String? = null

        fun addEntry(entry: Entry) {
            if (minKey == null)
                minKey = entry.first

            totalBytes += currentWriter.append(entry.first, entry.second)
            if (totalBytes >= config.maxSstableSize) {
                result.add(
                    SSTableMetadata(
                        name = currentFile.toString(),
                        minKey = minKey!!,
                        maxKey = entry.first,
                        level = 1, // TODO fix
                        id = id,
                        fileSize = totalBytes
                    )
                )
                currentWriter.close()
                currentWriter = BinaryWriteAheadLogWriter(
                    currentFile.toFile()
                        .outputStream()
                        .buffered(config.maxSstableSize)
                )
                id = nextTableId()
                minKey = null
                currentFile = tableFile(rootDirectory, config.sstablePrefix, id)
            }
        }

        for (entry in seq) {
            addEntry(entry)
        }

        return result
    }

    override fun doCompaction() {
        TODO("Not yet implemented")
    }

    override fun addTableAsync(wal: Path) {
        threadPool.submit {
            addTable(wal)
        }
    }

    override fun close() {
        threadPool.shutdown()
    }

    private fun nextTableId(): Int = synchronized(this) {
        nextFile(rootDirectory, config.sstablePrefix)
    }

    private fun overwriteTableFile(dest: Path, source: Path) {
        val reader = BinaryWriteAheadLogReader(source)
        val os = dest.toFile()
            .outputStream()
            .buffered(Files.size(source).toInt())
        val writer = BinaryWriteAheadLogWriter(os)
        reader.read().forEach {
            writer.append(it.first, it.second)
        }
    }

    /**
     * Search the young level (level 0) for a key. Tables in the young level may have overlapping keys.
     */
    private fun getYoung(key: String): Record? =
        manifest.tables()[0]
            ?.values
            ?.filter { it.keyRange.contains(key) }
            ?.maxBy { it.id }
            ?.let {
                tableReader.read(it, key)
            }

    /**
     * Search all the levels except level 0 for a key. The search proceeds top down, so the newest occurrence of the key
     * will be returned, if the key exists in the database. This should be optimized later by adding a Bloom Filter per
     * level, or something similar.
     *
     * In the worst case, this function will run in proportion to the number of tables in the database. It could scan
     * the metadata of every table, though it will only read the contents of at most one table - the youngest table
     * containing the desired key.
     */
    private fun getOld(key: String): Record? {
        manifest.tables()
            .forEach { (_, tableMetas) ->
                tableMetas
                    .values
                    .forEach { it ->
                        if (it.keyRange.contains(key)) {
                            return tableReader.read(it, key)
                        }
                    }
            }

        return null
    }

    fun SSTableMetadata.tableFile(): Path = tableFile(rootDirectory, config.sstablePrefix, id)
}

private fun tableFile(rootDirectory: File, prefix: String, id: Int): Path =
    File(
        rootDirectory,
        "$prefix$id"
    ).toPath()


class BinarySSTableReader(
    private val rootDirectory: File,
    private val prefix: String
) : SSTableReader {
    override fun read(table: SSTableMetadata, key: String): Record? {
        val file = tableFile(rootDirectory, prefix, table.id)
        val reader = BinaryWriteAheadLogReader(file)

        return reader.read()
            .firstOrNull { it.first == key }
            ?.second
    }

}
