package com.lsmt.core

import com.lsmt.overlaps
import com.lsmt.table.SSTableMetadata
import com.lsmt.table.TableKey
import mu.KotlinLogging
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
     * Return all tables whose keys overlap the range.
     */
    fun getRange(key: KeyRange): List<SSTableMetadata>

    /**
     * Return the next candidate that should be merged into the next level. LevelDB proceeds round-robin through the key
     * range.
     * https://github.com/google/leveldb/blob/master/doc/impl.md#compactions
     */
    fun nextCompactionCandidate(): SSTableMetadata?

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

    /**
     * Remove all tables in this level.
     */
    fun clear()

    /**
     * Return a list of all the member tables, ordered by their key in the underlying structure
     */
    fun asList(): List<SSTableMetadata>
}

/**
 * Naive implementation of the level structure, backed by a SortedMap. The get operation runs in O(N).
 */
class StandardLevel(
    private val id: Int,
    private val map: TreeMap<TableKey, SSTableMetadata> = TreeMap()
) : Level {

    var lastCompaction: SSTableMetadata? = null

    override fun add(table: SSTableMetadata) {
        map[table.key] = table
    }

    override fun remove(table: SSTableMetadata) {
        map.remove(table.key)
    }

    override fun get(key: String): List<SSTableMetadata> {
        val candidates = map.values.filter { it.keyRange.contains(key) }
        logger.debug { "StandardLevel.get() level=$id levelSize=${map.size} candidates=${candidates.size}" }
        return candidates
    }

    override fun getRange(key: KeyRange): List<SSTableMetadata> = map.values.filter { it.keyRange overlaps key }

    override fun nextCompactionCandidate(): SSTableMetadata? = synchronized(this) {
        lastCompaction = if (lastCompaction == null) {
            map.firstEntry().value
        } else {
            map.higherEntry(lastCompaction?.key)?.value
        }

        // If it's still null at this point, we reached the maximum value in the map and should wrap back to the
        // beginning.
        if (lastCompaction == null) {
            lastCompaction = map.firstEntry().value
        }

        return lastCompaction
    }

    override fun size(): Int = map.size

    override fun copy(): Level = StandardLevel(id, TreeMap(map))

    override fun addAll(otherLevel: Level) {
        otherLevel.forEach { map[it.key] = it }
    }

    override fun clear() = map.clear()

    override fun asList(): List<SSTableMetadata> = map.values.toList()

    override fun iterator(): Iterator<SSTableMetadata> = map.values.iterator()

    companion object {
        val logger = KotlinLogging.logger {}
    }
}

object EmptyLevel : Level by StandardLevel(-1)

fun emptyLevel(): Level = EmptyLevel
