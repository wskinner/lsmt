import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.File
import java.io.FileOutputStream

interface WriteAheadLogManager : LSMRunnable {
    // Append a record to the log.
    fun append(key: String, value: Map<String, Any>)

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
    private lateinit var fos: FileOutputStream

    override fun append(key: String, value: Map<String, Any>) {
        fos.write(key.toByteArray(CHARSET))
        fos.write(SEPARATOR)
        fos.write(jacksonObjectMapper().writeValueAsBytes(value))
        fos.write(LINE_SEPARATOR)
//        fos.fd.sync()
    }

    override fun restore(): MemTable {
        TODO("Not yet implemented")
    }

    override fun start() {
        fos = FileOutputStream(file, true)
    }

    override fun stop() {
        fos.close()
    }

    companion object {
        val CHARSET = Charsets.UTF_8
        val SEPARATOR = ",".toByteArray(CHARSET)
        val LINE_SEPARATOR = "\n".toByteArray(CHARSET)
    }
}
