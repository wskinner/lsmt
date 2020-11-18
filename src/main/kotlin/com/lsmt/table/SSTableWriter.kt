package com.lsmt.table

import com.lsmt.core.Record
import com.lsmt.log.BinaryLogWriter
import com.lsmt.toByteArray
import java.io.OutputStream
import java.lang.Integer.min

/**
 * Entries must be written to this class in sorted order.
 */
class SSTableWriter(private val out: OutputStream) : BinaryLogWriter(out) {
    var dataLength = -1

    // After writing all blocks, this will contain the min key that begins in each block
    private val blockBounds = HashMap<Int, String>()

    override fun append(key: String, value: Record?): Int {
        val currentBlock = totalBytes / BLOCK_SIZE
        blockBounds.putIfAbsent(currentBlock, key)
        return super.append(key, value)
    }

    override fun close() {
        if (! super.closed) {
            dataLength = totalBytes
            writeBlockIndex()
            writeFooter(dataLength)
            super.close()
        }
    }

    private fun writeBytes(bytes: ByteArray) {
        os.write(bytes)
        totalBytes += bytes.size
    }

    /**
     * The block index offset and length can be inferred from the data length.
     * Footer := dataLength
     * dataLength := int32
     */
    private fun writeFooter(dataLength: Int) {
        writeBytes(dataLength.toByteArray())
    }

    /**
     * BlockIndex := BlockHandle*
     * BlockHandle := offset length key
     * offset := int32
     * length := int32
     * key := int32
     */
    private fun writeBlockIndex() {
        for ((blockId, minKey) in blockBounds) {
            val blockOffset = blockId * BLOCK_SIZE
            val length = if ((blockId + 1 ) * BLOCK_SIZE > dataLength) {
                dataLength % BLOCK_SIZE
            } else {
                BLOCK_SIZE
            }
            writeBytes(blockOffset.toByteArray())
            writeBytes(length.toByteArray())
            writeBytes(minKey.length.toByteArray())
            writeBytes(minKey.toByteArray())
        }
    }
}
