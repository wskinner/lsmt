package table

import com.lsmt.Config
import com.lsmt.core.Entry
import com.lsmt.core.LogStructuredMergeTree
import com.lsmt.core.StandardLevel
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.SynchronizedFileGenerator
import com.lsmt.log.createLogReader
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
                BinaryLogWriter(manifestFile.outputStream())
            ),
            BinaryManifestReader(
                createLogReader(manifestFile.toPath())
            ),
            levelFactory = { StandardLevel(it) }
        )
        val tree = tree(manifest)
        fillTree(tree)
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
    val walFileGenerator = SynchronizedFileGenerator(walDir, Config.walPrefix)
    val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)

    val tableController = StandardSSTableController(
        Config.maxSstableSize,
        manifestManager,
        TableCache(
            BinarySSTableReader(sstableDir),
            Config.maxCacheSizeMB,
            walFileGenerator = walFileGenerator,
            sstableFileGenerator = sstableFileGenerator
        ),
        SynchronizedFileGenerator(sstableDir, Config.sstablePrefix)
    )

    val tableManager = StandardSSTableManager(
        sstableDir,
        manifestManager,
        BinarySSTableReader(sstableDir),
        Config,
        tableController
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        tableManager,
        BinaryLogManager(walFileGenerator),
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
