package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.core.Key
import com.lsmt.core.Record
import com.lsmt.readInt
import mu.KotlinLogging
import java.nio.ByteBuffer
import java.nio.ByteOrder

interface TableBuffer {
    fun get(key: Key): Record?

    fun iterator(): Iterator<Entry>
}

class StandardTableBuffer(
    private val delegate: ByteBuffer,
    val table: SSTableMetadata
) : TableBuffer {

    private val dataLimit = dataLength()

    init {
        delegate.order(ByteOrder.LITTLE_ENDIAN)
        delegate.position(0)
    }

    private val reader: ThreadLocal<TableBufferReader> = ThreadLocal.withInitial {
        TableBufferReader(
            delegate.asReadOnlyBuffer(),
            table,
            dataLimit
        )
    }

    override fun get(key: Key): Record? = reader
        .get()
        .get(key)

    private fun dataLength(): Int {
        val currentPosition = delegate.position()
        delegate.position(delegate.limit() - 4)
        val length = delegate.readInt()
        delegate.position(currentPosition)
        return length
    }

    override fun iterator(): Iterator<Entry> = SSTableIterator(delegate, dataLength())

    companion object {
        val logger = KotlinLogging.logger {}
    }
}
