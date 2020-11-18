package log

import core.Compactor
import table.ManifestManager
import table.SSTableController

/**
 * log.Compactor that implements the LevelDB compaction strategy, which is described at
 * https://github.com/google/leveldb/blob/master/doc/impl.md#compactions. Parts of this comment are copied verbatim
 * from that page.
 *
 * Level 0 compactions are a special case and are not handled here. This class only handles compactions at higher
 * levels.
 *
 * When the size of level L exceeds its limit, we compact it in a background thread. The compaction picks a file from
 * level L and all overlapping files from the next level L+1. Note that if a level-L file overlaps only part of a
 * level-(L+1) file, the entire file at level-(L+1) is used as an input to the compaction and will be discarded after
 * the compaction.
 *
 * A compaction merges the contents of the picked files to produce a sequence of level-(L+1) files. We switch to
 * producing a new level-(L+1) file after the current output file has reached the target file size (2MB). We also
 * switch to a new output file when the key range of the current output file has grown enough to overlap more than ten
 * level-(L+2) files. This last rule ensures that a later compaction of a level-(L+1) file will not pick up too much
 * data from level-(L+2).
 */
class StandardCompactor(
    private val manifest: ManifestManager,
    private val levelSizeLimit: (Int) -> Int,
    private val ssTableController: SSTableController
) : Compactor {

    override fun run() {
        for ((index, level) in manifest.levels().filterNot { it.key == 0 }) {
            if (level.size() > levelSizeLimit(index)) {
                doCompaction(index)
            }
        }
    }

    /**
     * Compact level i  into level i + 1. Level i is guaranteed to exist, but level i + 1 may not.
     */
    private fun doCompaction(levelI: Int) = synchronized(manifest) {
        val l1 = manifest.level(levelI)

        while (l1.size() > levelSizeLimit(levelI)) {
            ssTableController.merge(levelI)
        }
    }
}
