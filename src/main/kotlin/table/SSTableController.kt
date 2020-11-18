package table

import core.Entry
import core.NumberedFile
import core.entries
import core.nextFile
import log.BinaryWriteAheadLogWriter
import log.createLogReader
import mu.KotlinLogging
import java.io.File
import java.nio.file.Files
import java.nio.file.Path

interface SSTableController {

    /**
     * Merge a single table from level i and 0 or more tables from level i + 1 into level i + 1.
     * Returns: list of new tables. All the input tables are no longer used and should be removed from the index.
     */
    fun merge(tables: Sequence<SSTableMetadata>, targetLevel: Int): List<SSTableMetadata>?

    /**
     * Create a new table file from the log and return the metadata. The caller is responsible for updating the index.
     */
    fun addTableFromLog(logPath: Path): SSTableMetadata
}

class StandardSSTableController(
    private val rootDirectory: File,
    private val tablePrefix: String,
    private val maxTableSize: Int
) : SSTableController {

    private val tableLock = "lock"

    /**
     * Do an N-way merge of all the entries from all N tables. The tables are already in sorted order, but it's possible
     * that some keys are present in both the older and the younger level. In that case, we must drop the older values
     * in favor of the younger ones which supersede them. TODO
     */
    override fun merge(tables: Sequence<SSTableMetadata>, targetLevel: Int): List<SSTableMetadata>? {
        val start = System.nanoTime()
        try {
            val ids = tables.toList().map { it.id }
            logMergeTask(ids, targetLevel, "started")
            val result = mutableListOf<SSTableMetadata>()
            val seq = entries(tables)

            var (id, currentFile) = nextTableFile()
            var currentWriter = BinaryWriteAheadLogWriter(
                currentFile.toFile()
                    .outputStream()
                    .buffered(maxTableSize)
            )
            var totalBytes = 0
            var minKey: String? = null

            fun addEntry(entry: Entry) {
                if (minKey == null)
                    minKey = entry.first

                totalBytes += currentWriter.append(entry.first, entry.second)
                if (totalBytes >= maxTableSize) {
                    result.add(
                        SSTableMetadata(
                            path = currentFile.toString(),
                            minKey = minKey!!,
                            maxKey = entry.first,
                            level = targetLevel,
                            id = id,
                            fileSize = totalBytes
                        )
                    )
                    totalBytes = 0
                    currentWriter.close()
                    currentWriter = BinaryWriteAheadLogWriter(
                        currentFile.toFile()
                            .outputStream()
                            .buffered(maxTableSize)
                    )
                    minKey = null
                    val nextFile = nextTableFile()
                    id = nextFile.first
                    currentFile = nextFile.second
                }
            }

            for (entry in seq) {
                addEntry(entry)
            }

            logMergeTask(ids, targetLevel, "complete", System.nanoTime() - start)
            return result
        } catch (t: Throwable) {
            logger.error(t) { "Error in merge()" }
            return null
        }
    }

    private fun logMergeTask(ids: List<Int>, targetLevel: Int, status: String, durationNanos: Long? = null) {
        val baseMessage = "MergeTask=$ids targetLevel=$targetLevel status=$status"
        val message = if (durationNanos != null) {
            val durationMillis = durationNanos / 1000000
            val durationSeconds = "%.2f".format(durationMillis / 1000.0)
            "$baseMessage duration=$durationSeconds"
        } else {
            baseMessage
        }
        logger.info { message }
    }

    override fun addTableFromLog(logPath: Path): SSTableMetadata {
        // 1. Create the new table file.
        val (id, file) = nextTableFile()
        // 2. Read the wal file, merge and sort its contents, and write the result to the new table file.
        val writer = BinaryWriteAheadLogWriter(
            file.toFile()
                .outputStream()
                .buffered(Files.size(logPath).toInt())
        )
        val data = try {
            val data = createLogReader(logPath).read().sortedBy { it.first }
            data
        } catch (e: Throwable) {
            throw e
        }

        var totalBytes = 0
        writer.use {
            data.forEach {
                totalBytes += writer.append(it.first, it.second)
            }
        }

        return SSTableMetadata(
            path = file.toString(),
            minKey = data.first().first,
            maxKey = data.last().first,
            level = 0,
            id = id,
            fileSize = totalBytes
        )
    }

    private fun nextTableId(): Int = nextFile(rootDirectory, tablePrefix)

    private fun nextTableFile(): NumberedFile = synchronized(tableLock) {
        val id = nextTableId()
        return id to tableFile(rootDirectory, tablePrefix, id)
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

fun tableFile(rootDirectory: File, prefix: String, id: Int): Path =
    File(
        rootDirectory,
        "$prefix$id"
    ).toPath()


