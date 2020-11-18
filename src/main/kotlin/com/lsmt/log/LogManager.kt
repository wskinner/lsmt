package com.lsmt.log

import com.lsmt.core.Key
import com.lsmt.core.Record
import com.lsmt.log.BinaryLogWriter.Companion.BLOCK_SIZE
import com.lsmt.table.SSTableWriter
import mu.KotlinLogging
import java.io.OutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

data class LogHandle(val id: Long, val totalBytes: Int)

interface LogManager : LogWriter {
    // Start a new log file. Return the ID of the old log file and the number of .
    fun rotate(): LogHandle
}

data class Header(val crc: Int, val length: Int, val type: Int)


/**
 *
 * The log file contents are a sequence of 32KB blocks. The only exception is that the tail of the file may contain a partial block.
 *
 * block := record* trailer?
 * record :=
 * checksum: int32     // crc32c of type and data[] ; little-endian
 * length: int32       // little-endian
 * type: uint8          // One of FULL, FIRST, MIDDLE, LAST
 * data: uint8[length]
 */
class BinaryLogManager(
    private val fileGenerator: FileGenerator,
    private inline val fileWriterFactory: (OutputStream) -> LogWriter
) : LogManager {
    var id: Long
        private set

    var filePath: Path
        private set

    init {
        val numberedFile = fileGenerator.next()
        id = numberedFile.first
        filePath = numberedFile.second
    }

    private var writer: LogWriter = createWriter(filePath)

    /**
     * Not thread safe.
     * 1. Convert the key and value to one or more records. Depending on the size of the data and how many bytes remain
     *    in the current block, the number of records will vary.
     * 2. Write the record(s) to the file
     *
     * The record header (checksum, length, type) requires 9 bytes. If the current block has less than 9 bytes remaining,
     * the remaining bytes must be zeroes.
     */
    override fun close() {
        writer.close()
    }

    override fun append(key: Key, value: Record?): Int = writer.append(key, value)

    override fun size(): Int = writer.size()

    override fun rotate(): LogHandle {
        val oldId = id
        close()
        val totalBytes = writer.totalBytes()
        val numberedFile = fileGenerator.next()
        id = numberedFile.first
        filePath = numberedFile.second
        writer = createWriter(filePath)

        return LogHandle(oldId, totalBytes)
    }

    override fun totalBytes(): Int = writer.totalBytes()

    private fun createWriter(path: Path): LogWriter {
        val os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
            .buffered(BLOCK_SIZE)
        return fileWriterFactory(os)
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

fun createLogReader(path: Path): BinaryLogReader = BinaryLogReader(path)

fun createWalManager(fileGenerator: FileGenerator): BinaryLogManager =
    BinaryLogManager(fileGenerator) { BinaryLogWriter(it) }

fun createSSTableManager(fileGenerator: FileGenerator): BinaryLogManager =
    BinaryLogManager(fileGenerator) { SSTableWriter(it) }

