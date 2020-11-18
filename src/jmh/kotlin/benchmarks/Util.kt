package benchmarks

import ch.qos.logback.classic.Level
import com.lsmt.core.StandardLevel
import com.lsmt.core.StandardLogStructuredMergeTree
import com.lsmt.core.maxLevelSize
import com.lsmt.log.*
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

    val tableController = StandardSSTableController(
        config.maxSstableSize,
        manifestManager,
        SynchronizedFileGenerator(sstableDir, config.sstablePrefix)
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
            tableController,
            StandardCompactor(
                manifestManager,
                { maxLevelSize(it) },
                tableController
            )
        ),
        BinaryLogManager(
            SynchronizedFileGenerator(walDir, config.walPrefix)
        ),
        config,
        Level.DEBUG
    ).apply { start() }
}
