package table

import Config
import core.Record
import core.nextFile
import log.BinaryWriteAheadLogReader
import log.BinaryWriteAheadLogWriter
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
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

    override fun addTable(wal: Path, level: Int) {
        // 1. Create the new table file.
        val id = nextTableId()
        val file = tableFile(rootDirectory, config.sstablePrefix, id)

        // 2. Read the wal file, merge and sort its contents, and write the result to the new table file.
        val writer = BinaryWriteAheadLogWriter(
            file,
            file.toFile()
                .outputStream()
                .buffered(Files.size(wal).toInt())
        )
        val data = BinaryWriteAheadLogReader(wal).read()
        data.forEach {
            writer.append(it.first, it.second)
        }

        // 3. Add the new table file to the young level.
        manifest.addTable(
            SSTableMetadata(
                name = file.toString(),
                minKey = data.first().first,
                maxKey = data.last().first,
                level = level,
                id = id
            )
        )
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
        val writer = BinaryWriteAheadLogWriter(dest, os)
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
