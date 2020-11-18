package com.lsmt.table

import com.lsmt.core.maxLevelSize
import java.lang.Integer.max
import java.lang.Integer.min

data class MergeTask(
    val sourceTables: List<SSTableMetadata>
)

interface MergeStrategy {
    fun mergeTargets(level: Int, manifest: ManifestManager): MergeTask
}

/**
 * Compact a single non-young table.
 */
class StandardMergeStrategy : MergeStrategy {
    override fun mergeTargets(level: Int, manifest: ManifestManager): MergeTask {
        val sourceTables = if (level == 0) {
            manifest.level(level).asList()
        } else {
            val nextCompactionCandidate = manifest.level(level).nextCompactionCandidate()
            if (nextCompactionCandidate != null)
                listOf(nextCompactionCandidate)
            else
                emptyList()
        }

        return MergeTask(sourceTables)
    }
}

/**
 * Compact 5 tables, or 10% of the gap between the desired max level size and the actual level size.
 */
class AdaptiveCompactionStrategy : MergeStrategy {
    override fun mergeTargets(level: Int, manifest: ManifestManager): MergeTask {
        val sourceTables = if (level == 0) {
            manifest.level(level).asList()
        } else {
            val numTables = max(1, min(5, ((manifest.level(level).size() - maxLevelSize(level)) / 10.0).toInt()))
            0.until(numTables).mapNotNull { manifest.level(level).nextCompactionCandidate() }
        }

        return MergeTask(sourceTables)
    }
}
