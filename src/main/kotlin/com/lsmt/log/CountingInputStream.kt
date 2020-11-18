package com.lsmt.log

import java.io.InputStream

/**
 * InputStream that counts the number of bytes that have been read. Not thread safe.
 */
class CountingInputStream(private val delegate: InputStream) : InputStream() {
    var bytesRead = 0
    private val totalBytes = delegate.available()

    override fun read(): Int {
        bytesRead++
        return delegate.read()
    }

    override fun readNBytes(len: Int): ByteArray {
        bytesRead += len
        return delegate.readNBytes(len)
    }

    override fun readAllBytes(): ByteArray {
        val result = delegate.readAllBytes()
        bytesRead = result.size
        return result
    }

    override fun available(): Int {
        return totalBytes - bytesRead
    }
}
