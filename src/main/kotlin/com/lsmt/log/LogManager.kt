package com.lsmt.log

import com.lsmt.core.Entry
import com.lsmt.core.Record
import com.lsmt.log.BinaryLogWriter.Companion.BLOCK_SIZE
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

interface LogManager : LogWriter {
    // Start a new log file. Return the ID of the old log file.
    fun rotate(): Long

    // Read the current log file and return its contents merged and sorted.
    fun read(): List<Entry>
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
    private val fileGenerator: FileGenerator
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

    override fun append(key: String, value: Record?): Int = writer.append(key, value)

    override fun size(): Int = writer.size()

    override fun rotate(): Long {
        val oldId = id
        close()
        val numberedFile = fileGenerator.next()
        id = numberedFile.first
        filePath = numberedFile.second
        writer = createWriter(filePath)
        return oldId
    }

    override fun read(): List<Entry> = BinaryLogReader(filePath) { it.decode() }.readAll()

    private fun createWriter(path: Path): LogWriter {
        val os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
            .buffered(BLOCK_SIZE)
        return BinaryLogWriter(os)
    }

    companion object {
        val logger = KotlinLogging.logger { }
    }
}

fun createLogReader(path: Path): BinaryLogReader<Entry> = BinaryLogReader(path) { it.decode() }

