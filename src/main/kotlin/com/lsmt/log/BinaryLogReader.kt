package com.lsmt.log

import com.lsmt.counting
import com.lsmt.domain.Entry
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path

/**
 * Reader class for WAL files.
 */
class BinaryLogReader(
    private val filePath: Path
) {

    fun readAll(): List<Entry> = read().toList()

    fun read(): Sequence<Entry> = sequence {
        Files.newInputStream(filePath).counting().use {
            try {
                while (it.available() >= 9) {
                    val record = it.readEntry()
                    yield(record)
                }
            } catch (e: Throwable) {
                logger.error(e) { "BinaryLogReader.read(): Error" }
            }
        }
    }


    companion object {
        val logger = KotlinLogging.logger { }
    }
}
