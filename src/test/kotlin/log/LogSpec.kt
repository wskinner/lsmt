package log

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.util.*

class LogSpec : StringSpec({
    "log serialization and deserialization of a single full record" {
        val file = createTempFile()
        val wal = BinaryWriteAheadLogManager(file.toPath())
        wal.start()
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
        val file = createTempFile()
        val wal = BinaryWriteAheadLogManager(file.toPath())
        wal.start()
        val key = "foobar"
        val value = TreeMap<String, Any>()

        val randomBytes = ByteArray(BinaryWriteAheadLogManager.blockSize * 3 + 25)
        Random().nextBytes(randomBytes)
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
        val file = createTempFile()
        val wal = BinaryWriteAheadLogManager(file.toPath())
        wal.start()
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
})
