import Bytes.longToBytes
import com.dslplatform.json.DslJson
import java.io.BufferedOutputStream
import java.io.File
import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.StandardOpenOption

interface WriteAheadLogManager : LSMRunnable {
    // Append a record to the log.
    fun append(key: String, value: Map<String, Any>): Long

    // Restore the memtable from the log
    fun restore(): MemTable
}

/**
 * The file contains one record per line. Each record consists of a key, a comma, and a JSON serialized value. For
 * example:
 * my_key,{"test": 1234}
 */
class StandardWriteAheadLogManager(
    private val file: File
) : WriteAheadLogManager {
    private val dslJson = DslJson<Any>()
    private lateinit var bos: BufferedOutputStream
    private var sequence: Long = -1

    // Not thread safe!
    override fun append(key: String, value: Map<String, Any>): Long {
        val seq = sequence++
        bos.write(longToBytes(seq))
        bos.write(key.toByteArray(CHARSET))
        bos.write(SEPARATOR)
        dslJson.serialize(value, bos)
        bos.write(LINE_SEPARATOR)
        bos.flush()
        return seq
    }

    override fun restore(): MemTable {
        TODO("Not yet implemented")
    }

    override fun start() {
        sequence = if (!file.exists()) {
            file.createNewFile()
            0
        } else {
            lastSequenceNumber() + 1
        }

        bos = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND).buffered()
    }

    override fun stop() {
        bos.close()
    }

    // Seek to the end of the file, then backwards until we find the last line separator. Then read the sequence number,
    // which is the first 8 bytes of each line.
    private fun lastSequenceNumber(): Long {
        val randomAccessFile = RandomAccessFile(file, "r")
        var offset = randomAccessFile.length()
        randomAccessFile.seek(offset)
        val separator = LINE_SEPARATOR[0].toChar()
        while (randomAccessFile.read().toChar() != separator) {
            offset -= 2
            randomAccessFile.seek(offset)
        }
        val bytes = ByteArray(8)
        randomAccessFile.read(bytes)
        return Bytes.bytesToLong(bytes)
    }

    companion object {
        val CHARSET = Charsets.UTF_8
        val SEPARATOR = ",".toByteArray(CHARSET)
        val LINE_SEPARATOR = "\n".toByteArray(CHARSET)
    }
}
