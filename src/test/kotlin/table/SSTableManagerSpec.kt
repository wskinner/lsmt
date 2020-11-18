package table

import ch.qos.logback.classic.Level.DEBUG
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
        (manifest.level(0).size()) shouldBe 1
        (manifest.level(1).size()) shouldBe 10
    }

    "no exception" {
        val random = Random(0)

        val tree = treeFactory()
        for (i in 1..100) {
            val value = ByteArray(random.nextInt(1000) + 5).apply {
                random.nextBytes(this)
            }
            tree.put(
                "person$i", value
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
        Config,
        DEBUG
    ).apply { start() }
}

fun fillTree(tree: LogStructuredMergeTree): List<Entry> {
    val random = Random(0)
    val entries = mutableListOf<Entry>()

    for (i in 0..150_000) {
        val key = "key$i"
        val value = ByteArray(random.nextInt(200) + 25).apply {
            random.nextBytes(this)
        }

        tree.put(key, value)
        entries.add(key to value)
    }
    return entries
}
