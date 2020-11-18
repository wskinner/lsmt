package com.lsmt.table

import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(private val delegate: ByteBuffer) : InputStream() {
    override fun read(): Int {
        return delegate.get().toInt()
    }

    override fun readNBytes(len: Int): ByteArray {
        val result = ByteArray(len)
        delegate.get(result)
        return result
    }

    override fun readAllBytes(): ByteArray {
        val length = delegate.limit() - delegate.position()
        return readNBytes(length)
    }

    override fun available(): Int {
        return delegate.limit() - delegate.position()
    }
}
