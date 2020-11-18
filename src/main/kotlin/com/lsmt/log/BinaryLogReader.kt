package com.lsmt.log

import com.lsmt.core.Entry
import com.lsmt.counting
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.CRC32C

class BinaryLogReader(
    private val filePath: Path
) {

    private val crc = CRC32C()

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
