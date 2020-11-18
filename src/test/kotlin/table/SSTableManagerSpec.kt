package table

import Config
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import log.BinaryWriteAheadLogManager
import log.BinaryWriteAheadLogReader
import log.BinaryWriteAheadLogWriter
import java.io.File
import java.nio.file.Path
import java.util.*

class SSTableManagerSpec : StringSpec({
    "merge level 0 to level 1" {
        val walDir = createTempDir()
        val sstableDir = createTempDir()
        val manifestDir = createTempDir()
        val manifestFile = createTempFile(directory = manifestDir)
        val manifest = StandardManifestManager(
            BinaryManifestWriter(
                BinaryWriteAheadLogWriter(manifestFile.outputStream())
            ),
            BinaryManifestReader(
                BinaryWriteAheadLogReader(manifestFile.toPath())
            )
        )
        val tableManager = StandardSSTableManager(
            sstableDir,
            manifest,
            BinarySSTableReader(sstableDir),
            Config
        )

        val wals = makeWals(walDir)

        for (wal in wals) {
            tableManager.addTable(wal)
        }

        (manifest.tables()[0]?.size ?: -1) shouldBe 0
    }
})

fun makeWals(walDir: File): List<Path> {
    val random = Random(0)
    val logManager = BinaryWriteAheadLogManager(walDir)
    val walPaths = mutableListOf<Path>()

    for (i in 0..4) {
        var bytes = 0
        while (bytes < Config.maxWalSize) {
            val key = "key$bytes"
            val value = TreeMap<String, Any>()
            val randomBytes = ByteArray(BinaryWriteAheadLogWriter.BLOCK_SIZE * random.nextInt(3) + 25)
            random.nextBytes(randomBytes)
            value["key0"] = 1
            value["key1"] = 10F
            value["key2"] = 100L
            value["key3"] = 1000.123
            value["key4"] = Base64.getEncoder().encodeToString(randomBytes)
            bytes += logManager.append(key, value)
        }
        walPaths.add(logManager.rotate())
    }

    return walPaths
}
