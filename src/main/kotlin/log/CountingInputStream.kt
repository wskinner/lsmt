package log

import java.io.InputStream

/**
 * InputStream that counts the number of bytes that have been read. Not thread safe.
 */
class CountingInputStream(private val delegate: InputStream) : InputStream() {
    var bytesRead = 0L

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
        bytesRead = result.size.toLong()
        return result
    }

    override fun available(): Int {
        return delegate.available()
    }
}
