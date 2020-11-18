package com.lsmt.log

import com.lsmt.core.longBytesSeq
import com.lsmt.log.BinaryLogWriter.Companion.BLOCK_SIZE
import com.lsmt.toKey
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.nio.file.Files
import java.util.*

class LogSpec : StringSpec({

    "log serialization and deserialization of a single full record" {
        val random = Random(0)
        val dir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(dir, "prefix")
        val wal = createWalManager(fileGenerator)
        val key = "foobar".toByteArray().toKey()
        val value = ByteArray(100).apply {
            random.nextBytes(this)
        }

        wal.use {
            wal.append(key, value)
        }

        val logHandle = wal.rotate()
        val reader = BinaryLogReader(fileGenerator.path(logHandle.id))

        val deserialized = reader.readAll().toMap()
        deserialized.size shouldBe 1
        deserialized[key] shouldBe value
    }

    "log serialization and deserialization of a multi part record" {
        val random = Random(0)

        val dir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(dir, "prefix")
        val wal = createWalManager(fileGenerator)
        val key = "foobar".toByteArray().toKey()
        val value = ByteArray(BLOCK_SIZE * 3 + 25).apply {
            random.nextBytes(this)
        }
        wal.use {
            wal.append(key, value)
        }

        val logHandle = wal.rotate()
        val reader = BinaryLogReader(fileGenerator.path(logHandle.id))

        val deserialized = reader.read().toMap()
        deserialized.size shouldBe 1
        deserialized[key] shouldBe value
    }

    "log serialization and deserialization of an empty record" {
        val dir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(dir, "prefix")
        val wal = createWalManager(fileGenerator)
        val key = "foobar".toByteArray().toKey()
        val value = ByteArray(0)
        wal.use {
            wal.append(key, value)
        }

        val logHandle = wal.rotate()
        val reader = BinaryLogReader(fileGenerator.path(logHandle.id))
        val deserialized = reader.read().toMap()
        deserialized.size shouldBe 1
        deserialized[key] shouldBe value
    }

    "log serialization and deserialization of several records" {
        val random = Random(0)
        val dir = createTempDir().apply { deleteOnExit() }
        val fileGenerator = SynchronizedFileGenerator(dir, "prefix")
        val wal = createWalManager(fileGenerator)

        val entries = longBytesSeq().take(10).map { key ->
            val value = ByteArray(BLOCK_SIZE * random.nextInt(3) + 25).apply {
                random.nextBytes(this)
            }
            key.toKey() to value
        }

        wal.use {
            entries.forEach {
                wal.append(it.first, it.second)
            }
        }
        val logHandle = wal.rotate()
        val reader = BinaryLogReader(fileGenerator.path(logHandle.id))

        val deserialized = reader.read()
        entries.asSequence().zip(deserialized).forEach {
            it.second.first shouldBe it.first.first
            it.second.second shouldBe it.second.second
        }
    }

    "trailer read and write including records spanning multiple blocks" {
        ((BLOCK_SIZE - 20)..(BLOCK_SIZE)).forEach { dataSize ->
            val random = Random(0)
            val file = createTempFile().apply { deleteOnExit() }
            val writer = BinaryLogWriter(
                Files.newOutputStream(file.toPath()).buffered(BLOCK_SIZE)
            )
            val reader = BinaryLogReader(file.toPath())

            val data = ByteArray(dataSize)
            random.nextBytes(data)

            val key = "key".toByteArray().toKey()
            writer.use {
                writer.append(key, data)
            }

            val result = reader.readAll()
            result.size shouldBe 1
            try {
                result.first().second shouldBe data
            } catch (t: Throwable) {
                t.printStackTrace()
            }
        }
    }

    "trailer read and write including records spanning multiple blocks (long)" {
        val dataSize = BLOCK_SIZE * 2 + 1
        val random = Random(0)
        val file = createTempFile().apply { deleteOnExit() }
        val writer = BinaryLogWriter(
            Files.newOutputStream(file.toPath()).buffered(BLOCK_SIZE)
        )
        val reader = BinaryLogReader(file.toPath())

        val data = ByteArray(dataSize)
        random.nextBytes(data)

        val key = "foo".toByteArray().toKey()
        writer.use {
            writer.append(key, data)
        }

        val result = reader.readAll()
        result.size shouldBe 1
        try {
            result.first().second shouldBe data
        } catch (t: Throwable) {
            t.printStackTrace()
        }
    }
})
