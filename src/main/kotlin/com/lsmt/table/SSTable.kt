package com.lsmt.table

import com.lsmt.cache.TableBuffer
import com.lsmt.domain.Entry
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import mu.KotlinLogging

/**
 * The SSTable format is inspired by the LevelDB format as described in
 * https://github.com/google/leveldb/blob/master/doc/table_format.md.
 *
 *  Files consist of a series of data blocks, an index block, some padding, and a fixed length footer which includes
 *  handle pointing to the index block.
 *
 *  file := datablock* indexblock padding footer
 *
 *  The key for a block in the block index is a string which is less than or equal to all keys in that block, but greater than all
 *  keys in the preceding block.
 *
 */

interface SSTable : Iterable<Entry> {
    fun get(key: Key): Record?
}

class StandardSSTable(
    private val byteBuffer: TableBuffer
) : SSTable {

    override fun get(key: Key): Record? = byteBuffer.get(key)

    override fun iterator(): Iterator<Entry> = byteBuffer.iterator()

    companion object {
        val logger = KotlinLogging.logger { }
    }

}
