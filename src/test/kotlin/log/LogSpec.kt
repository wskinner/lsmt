package log

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import log.BinaryWriteAheadLogWriter.Companion.BLOCK_SIZE
import java.nio.file.Files
import java.util.*

class LogSpec : StringSpec({

    "log serialization and deserialization of a single full record" {
        val file = createTempDir()
        val wal = BinaryWriteAheadLogManager(file)
        val key = "foobar"
        val value = TreeMap<String, Any>()
        wal.use {
            value["key1"] = 1
            value["key2"] = 1.234
            value["key3"] = "value"
            wal.append(key, value)
        }

        val deserialized = wal.read()
        deserialized.size shouldBe 1
        deserialized[0].first shouldBe key
        val deserializedValue = deserialized[0].second
        deserializedValue["key1"] shouldBe 1
        deserializedValue["key2"] shouldBe 1.234
        deserializedValue["key3"] shouldBe "value"
    }

    "log serialization and deserialization of a multi part record" {
        val random = Random(0)

        val file = createTempDir()
        val wal = BinaryWriteAheadLogManager(file)
        val key = "foobar"
        val value = TreeMap<String, Any>()

        val randomBytes = ByteArray(BLOCK_SIZE * 3 + 25)
        random.nextBytes(randomBytes)
        val value3 = Base64.getEncoder().encodeToString(randomBytes)
        wal.use {
            value["key1"] = 1
            value["key2"] = 1.234
            value["key3"] = value3
            wal.append(key, value)
        }

        val deserialized = wal.read()
        deserialized.size shouldBe 1
        deserialized[0].first shouldBe key
        val deserializedValue = deserialized[0].second
        deserializedValue["key1"] shouldBe 1
        deserializedValue["key2"] shouldBe 1.234
        deserializedValue["key3"] shouldBe value3
    }

    "log serialization and deserialization of an empty record" {
        val file = createTempDir()
        val wal = BinaryWriteAheadLogManager(file)
        val key = "foobar"
        val value = TreeMap<String, Any>()
        wal.use {
            wal.append(key, value)
        }

        val deserialized = wal.read()
        deserialized.size shouldBe 1
        deserialized[0].first shouldBe key
        val deserializedValue = deserialized[0].second
        deserializedValue.size shouldBe 0
    }

    "log serialization and deserialization of several records" {
        val random = Random(0)

        val file = createTempDir()
        val wal = BinaryWriteAheadLogManager(file)

        val entries = (0..100).map {
            val key = "key$it"
            val value = TreeMap<String, Any>()
            val randomBytes = ByteArray(BLOCK_SIZE * random.nextInt(3) + 25)
            random.nextBytes(randomBytes)
            value["key0"] = 1
            value["key1"] = 10F
            value["key2"] = 100L
            value["key3"] = 1000.123
            value["key4"] = Base64.getEncoder().encodeToString(randomBytes)
            key to value
        }.sortedBy { it.first }

        wal.use {
            entries.forEach {
                wal.append(it.first, it.second)
            }
        }

        val deserialized = wal.read()
        entries.zip(deserialized).forEach {
            it.second.first shouldBe it.first.first
            it.second.second shouldBe it.second.second
        }
    }

    "trailer read and write including records spanning multiple blocks" {
        ((BLOCK_SIZE - 10)..(BLOCK_SIZE + 10)).forEach { dataSize ->
            val random = Random(0)
            val file = createTempFile()
            val writer = BinaryWriteAheadLogWriter(
                Files.newOutputStream(file.toPath()).buffered(BLOCK_SIZE)
            )
            val reader = BinaryWriteAheadLogReader(file.toPath()) { it.readAllBytes() }

            val data = ByteArray(dataSize)
            random.nextBytes(data)

            writer.use {
                writer.appendBytes(data)
            }

            val result = reader.read()
            result.size shouldBe 1
            try {
                result.first() shouldBe data
            } catch (t: Throwable) {
                t.printStackTrace()
            }
        }
    }

    "trailer read and write including records spanning multiple blocks (long)" {
        val dataSize = BLOCK_SIZE * 2 + 1
        val random = Random(0)
        val file = createTempFile()
        val writer = BinaryWriteAheadLogWriter(
            Files.newOutputStream(file.toPath()).buffered(BLOCK_SIZE)
        )
        val reader = BinaryWriteAheadLogReader(file.toPath()) { it.readAllBytes() }

        val data = ByteArray(dataSize)
        random.nextBytes(data)

        writer.use {
            writer.appendBytes(data)
        }

        val result = reader.read()
        result.size shouldBe 1
        try {
            result.first() shouldBe data
        } catch (t: Throwable) {
            t.printStackTrace()
        }
    }
})
