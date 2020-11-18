package table

import Config
import concat
import core.*
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import log.BinaryWriteAheadLogManager
import log.BinaryWriteAheadLogReader
import log.BinaryWriteAheadLogWriter
import java.util.*

class SSTableManagerSpec : StringSpec({
    "merge level 0 to level 1" {
        val manifestDir = createTempDir()
        val manifestFile = createTempFile(directory = manifestDir)
        val manifest = StandardManifestManager(
            BinaryManifestWriter(
                BinaryWriteAheadLogWriter(manifestFile.outputStream())
            ),
            BinaryManifestReader(
                BinaryWriteAheadLogReader(manifestFile.toPath())
            ),
            levelFactory = { StandardLevel() }
        )
        val tree = tree(manifest)
        val entries = fillTree(tree)
        val tables = manifest.level(0)
            .sortedBy { it.id }
        val tableEntries = concat(tables)

        // The table entries have been merged and sorted, so we must do the same
        val mergedEntries = TreeMap<String, Record>()

        entries.forEach { mergedEntries[it.first] = it.second }
        tableEntries.forEach {
            it.second shouldBe mergedEntries[it.first]
        }

        (manifest.level(0).size()) shouldBe 0
    }
})

fun tree(manifestManager: ManifestManager): LogStructuredMergeTree {
    val walDir = createTempDir()
    val sstableDir = createTempDir()

    val tableManager = StandardSSTableManager(
        sstableDir,
        manifestManager,
        BinarySSTableReader(sstableDir),
        Config
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        tableManager,
        BinaryWriteAheadLogManager(
            walDir,
            Config.walPrefix
        ),
        Config
    ).apply { start() }
}

fun fillTree(tree: LogStructuredMergeTree): List<Entry> {
    val random = Random(0)
    val entries = mutableListOf<Entry>()

    for (i in 0..10000) {
        val key = "key$i"
        val value = TreeMap<String, Any>()
        val randomBytes = ByteArray(BinaryWriteAheadLogWriter.BLOCK_SIZE * random.nextInt(3) + 25)
        random.nextBytes(randomBytes)
        value["key0"] = 1
        value["key1"] = 10F
        value["key2"] = 100L
        value["key3"] = 1000.123
        value["key4"] = Base64.getEncoder().encodeToString(randomBytes)
        tree.put(key, value)
        entries.add(key to value)
    }
    return entries
}
