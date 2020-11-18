package com.lsmt.table

import com.lsmt.core.Record
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.SynchronizedFileGenerator
import com.lsmt.log.createSSTableManager
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.StringSpec
import java.util.*

class SSTableSpec : StringSpec({
    "read and write single record" {
        val random = Random(0)
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(sstableDir, "sstable")
        val logManager = createSSTableManager(fileGenerator)
        val expected = TreeMap<String, Record>()
        val data = ByteArray(100).apply {
            random.nextBytes(this)
        }
        val key = "key"
        expected[key] = data

        logManager.use { writer ->
            expected.forEach { writer.append(it.key, it.value) }
        }

        val logHandle = logManager.rotate()
        val tableMeta = SSTableMetadata(
            path = fileGenerator.path(logHandle.id).toString(),
            minKey = expected.firstKey(),
            maxKey = expected.lastKey(),
            level = 0,
            id = logHandle.id,
            fileSize = logHandle.totalBytes
        )

        val table = BinarySSTableReader().mmap(tableMeta)
        table.iterator().asSequence().zip(expected.asSequence()).forEach { (exp, actual) ->
            exp.first shouldBe actual.key
            exp.second shouldBe actual.value
        }
    }

    "read and write" {
        val random = Random(0)
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(sstableDir, "sstable")
        val logManager = createSSTableManager(fileGenerator)
        val expected = TreeMap<String, Record>()
        for (i in 0..1000) {
            val data = ByteArray(100).apply {
                random.nextBytes(this)
            }
            val key = "key$i"
            expected[key] = data
        }

        logManager.use { writer ->
            expected.forEach { writer.append(it.key, it.value) }
        }

        val logHandle = logManager.rotate()
        val tableMeta = SSTableMetadata(
            path = fileGenerator.path(logHandle.id).toString(),
            minKey = expected.firstKey(),
            maxKey = expected.lastKey(),
            level = 0,
            id = logHandle.id,
            fileSize = logHandle.totalBytes
        )

        val table = BinarySSTableReader().mmap(tableMeta)
        table.iterator().asSequence().zip(expected.asSequence()).forEach { (exp, actual) ->
            exp.first shouldBe actual.key
            exp.second shouldBe actual.value
        }
    }

    "table iterator" {
        val random = Random(0)
        val sstableDir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(sstableDir, "sstable")
        val logManager = createSSTableManager(fileGenerator)
        val expected = TreeMap<String, Record>()
        for (i in 0..1000) {
            val data = ByteArray(100).apply {
                random.nextBytes(this)
            }
            val key = "key$i"
            expected[key] = data
        }

        logManager.use { writer ->
            expected.forEach { writer.append(it.key, it.value) }
        }

        val logHandle = logManager.rotate()
        val tableMeta = SSTableMetadata(
            path = fileGenerator.path(logHandle.id).toString(),
            minKey = expected.firstKey(),
            maxKey = expected.lastKey(),
            level = 0,
            id = logHandle.id,
            fileSize = logHandle.totalBytes
        )

        val table = BinarySSTableReader().mmap(tableMeta)
        val iterator = table.iterator()
        val expectedEntries = expected.iterator()
        while (iterator.hasNext()) {
            val exp = expectedEntries.next()
            val cis = iterator.next()
            val get = table.get(exp.key)

            cis.first shouldBe exp.key
            cis.second shouldBe exp.value
            get shouldBe exp.value
        }
    }
})
