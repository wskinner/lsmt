package com.lsmt.cache

import com.lsmt.domain.*
import com.lsmt.manifest.SSTableMetadata
import com.lsmt.readInt
import com.lsmt.readNBytes
import com.lsmt.table.SSTableIterator
import com.lsmt.toKey
import java.nio.ByteBuffer

class TableBufferReader(
    private val delegate: ByteBuffer,
    private val table: SSTableMetadata,
    private val dataLimit: Int
) : TableBuffer {

    // TODO is this using contentEquals to compare?
    private val lruIndex = LRUPreCache<Key, BlockHandle>(10, readBlockIndex())
//    private val lruIndex = readBlockIndex()

    override fun get(key: Key): Record? {
        val closestBlock = lruIndex.floorEntry(key).value
        return if (closestBlock == null) {
            null
        } else {
            getInBlock(key, closestBlock)
        }
    }

    /**
     * Search a block for the given key. If there is a record with this key in this block,  return the value. If there
     * is no record, or the record is a delete, return null.
     *
     * TODO (will) optimize this a bit.
     */
    private fun getInBlock(targetKey: Key, block: BlockHandle): Record? {
        val iterator = StandardKeyIterator(delegate, block.offset, dataLimit)
        val found = iterator.seek(targetKey)
        if (found == null || found == false) {
            return null
        }
        lruIndex[targetKey] = BlockHandle(iterator.currentRecordOffset())
        return iterator.read()
    }

    override fun iterator(): Iterator<Entry> = SSTableIterator(delegate, dataLimit)

    private fun readIndexEntry(): Pair<Key, BlockHandle> {
        val offset = delegate.readInt()
        val keyLength = delegate.readInt()
        val key = delegate.readNBytes(keyLength)
        return key.toKey() to BlockHandle(offset)
    }

    private fun readBlockIndex(): BlockIndex {
        val index = BlockIndex()
        try {
            val indexStart = dataLimit
            delegate.position(indexStart)
            while (delegate.position() < delegate.limit() - 4) {
                val indexEntry = readIndexEntry()
                index[indexEntry.first] = indexEntry.second
            }
        } catch (e: Throwable) {
            StandardTableBuffer.logger.error(e) { "Failed to read block index table=${table.path} size=${table.fileSize}" }
            throw e
        }

        return index
    }

}
