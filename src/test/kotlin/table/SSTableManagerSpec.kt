package table

import com.lsmt.Config
import com.lsmt.core.*
import com.lsmt.log.*
import com.lsmt.merge
import com.lsmt.table.*
import com.lsmt.treeFactory
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.util.*

class SSTableManagerSpec : StringSpec({
    "merge level 0 to level 1" {
        val manifestDir = createTempDir().apply { deleteOnExit() }
        val manifestFile = createTempFile(directory = manifestDir).apply { deleteOnExit() }
        val manifest = StandardManifestManager(
            BinaryManifestWriter(
                BinaryWriteAheadLogWriter(manifestFile.outputStream())
            ),
            BinaryManifestReader(
                createLogReader(manifestFile.toPath())
            ),
            levelFactory = { StandardLevel(it) }
        )
        val tree = tree(manifest)
        val entries = fillTree(tree)
        val tables = manifest.level(0)
            .sortedBy { it.id }
        val tableEntries = entries(tables)

        // The table entries have been merged and sorted, so we must do the same
        val mergedEntries = entries.merge()

        tableEntries.forEach {
            it.second shouldBe mergedEntries[it.first]
        }

        tree.close()

        (manifest.level(0).size()) shouldBe 2
    }

    "test" {
        val random = Random(0)

        val tree = treeFactory()
        for (i in 1..100) {
            val name = ByteArray(random.nextInt(1000) + 5).run {
                random.nextBytes(this)
                Base64.getEncoder().encodeToString(this)!!
            }
            tree.put(
                "person$i", sortedMapOf(
                    "name" to name,
                    "age" to random.nextInt()
                )
            )
        }
        tree.close()
    }
})

fun tree(manifestManager: ManifestManager): LogStructuredMergeTree {
    val walDir = createTempDir().apply { deleteOnExit() }
    val sstableDir = createTempDir().apply { deleteOnExit() }


    val tableController = StandardSSTableController(
        Config.maxSstableSize,
        manifestManager,
        SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)
    )
    val compactor = StandardCompactor(
        manifestManager,
        { maxLevelSize(it) },
        tableController
    )
    val tableManager = StandardSSTableManager(
        sstableDir,
        manifestManager,
        BinarySSTableReader(sstableDir),
        Config,
        tableController,
        compactor
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        tableManager,
        BinaryWriteAheadLogManager(
            SynchronizedFileGenerator(walDir, Config.walPrefix)
        ),
        Config
    ).apply { start() }
}

fun fillTree(tree: LogStructuredMergeTree): List<Entry> {
    val random = Random(0)
    val entries = mutableListOf<Entry>()

    for (i in 0..100_000) {
        val key = "key$i"
        val value = TreeMap<String, Any>()
        val randomBytes = ByteArray(random.nextInt(200) + 25)
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
