package benchmarks

import ch.qos.logback.classic.Level
import com.lsmt.core.StandardLevel
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.log.BinaryLogManager
import com.lsmt.log.BinaryLogWriter
import com.lsmt.log.SynchronizedFileGenerator
import com.lsmt.log.createLogReader
import com.lsmt.parseConfig
import com.lsmt.table.*
import java.util.*

fun treeFactory(): StandardLogStructuredMergeTree {
    val manifestFile = createTempFile("manifest").apply { deleteOnExit() }
    val sstableDir = createTempDir("sstable").apply { deleteOnExit() }
    val walDir = createTempDir("wal").apply { deleteOnExit() }

    val config = parseConfig()

    val manifestManager = StandardManifestManager(
        BinaryManifestWriter(
            BinaryLogWriter(manifestFile.outputStream())
        ),
        BinaryManifestReader(
            createLogReader(manifestFile.toPath())
        ),
        levelFactory = { StandardLevel(it) }
    )

    val sstableFileGenerator = SynchronizedFileGenerator(sstableDir, config.sstablePrefix)
    val walFileGenerator = SynchronizedFileGenerator(walDir, config.walPrefix)
    val tableCache = TableCache(
        BinarySSTableReader(
            sstableDir,
            config.sstablePrefix
        ),
        config.maxCacheSizeMB,
        walFileGenerator = walFileGenerator,
        sstableFileGenerator = sstableFileGenerator
    )

    val tableController = StandardSSTableController(
        config.maxSstableSize,
        manifestManager,
        tableCache,
        sstableFileGenerator
    )

    return StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        StandardSSTableManager(
            sstableDir,
            manifestManager,
            BinarySSTableReader(
                sstableDir,
                config.sstablePrefix
            ),
            config,
            tableController
        ),
        BinaryLogManager(walFileGenerator),
        config,
        Level.INFO
    ).apply { start() }
}
