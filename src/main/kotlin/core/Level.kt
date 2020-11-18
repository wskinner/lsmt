package core

import table.SSTableMetadata
import table.TableKey
import java.util.*

interface Level : Iterable<SSTableMetadata> {
    /**
     * Add a table to the level
     */
    fun add(table: SSTableMetadata)

    /**
     * Remove a table from the level.
     */
    fun remove(table: SSTableMetadata)

    /**
     * Return all tables whose key ranges overlap the key.
     * This operation should be backed by an interval tree and run in O(log n) TODO.
     */
    fun get(key: String): List<SSTableMetadata>

    /**
     * Return the next candidate that should be merged into the next level. LevelDB proceeds round-robin through the key
     * range.
     * https://github.com/google/leveldb/blob/master/doc/impl.md#compactions
     */
    fun nextCompactionCandidate(): SSTableMetadata

    /**
     * Return the number of tables in the level.
     */
    fun size(): Int

    /**
     * Return a copy of this level.
     */
    fun copy(): Level

    /**
     * Add all the tables from otherLevel into this level. This may result in overlapping key ranges, violating the
     * level invariant for levels greater than 0, so in the usual case this should be followed by rebalancing.
     */
    fun addAll(otherLevel: Level)
}

/**
 * Naive implementation of the level structure, backed by a SortedMap. The get operation runs in O(N).
 */
class StandardLevel(
    private val map: TreeMap<TableKey, SSTableMetadata> = TreeMap<TableKey, SSTableMetadata>()
) : Level {

    override fun add(table: SSTableMetadata) {
        map[table.key] = table
    }

    override fun remove(table: SSTableMetadata) {
        map.remove(table.key)
    }

    override fun get(key: String): List<SSTableMetadata> = map.values.filter { it.keyRange.contains(key) }

    override fun nextCompactionCandidate(): SSTableMetadata {
        TODO("Not yet implemented")
    }

    override fun size(): Int = map.size

    override fun copy(): Level = StandardLevel(TreeMap(map))

    override fun addAll(otherLevel: Level) {
        otherLevel.forEach { map[it.key] = it }
    }

    override fun iterator(): Iterator<SSTableMetadata> = map.values.iterator()

}

object EmptyLevel : Level by StandardLevel(TreeMap())

fun emptyLevel(): Level = EmptyLevel
