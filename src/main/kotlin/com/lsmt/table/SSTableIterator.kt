package com.lsmt.table

import com.lsmt.core.Entry
import com.lsmt.counting
import com.lsmt.inputStream
import java.nio.ByteBuffer

class SSTableIterator(
    buffer: ByteBuffer,
    private val dataLength: Int
) : Iterator<Entry> {
    private val inputStream = buffer.asReadOnlyBuffer().apply { limit(dataLength) }
        .inputStream()
        .counting()

    override fun hasNext(): Boolean = inputStream.available() > 0

    override fun next(): Entry = inputStream.readEntry()

}
