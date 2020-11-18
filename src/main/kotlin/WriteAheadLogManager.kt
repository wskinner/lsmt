import com.dslplatform.json.DslJson
import java.io.BufferedOutputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption

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
    private val dslJson = DslJson<Any>()
    private lateinit var bos: BufferedOutputStream

    override fun append(key: String, value: Map<String, Any>) {
        bos.write(key.toByteArray(CHARSET))
        bos.write(SEPARATOR)
        dslJson.serialize(value, bos)
        bos.write(LINE_SEPARATOR)
        bos.flush()
    }

    override fun restore(): MemTable {
        TODO("Not yet implemented")
    }

    override fun start() {
        if (!file.exists())
            file.createNewFile()

        bos = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND).buffered()
    }

    override fun stop() {
        bos.close()
    }

    companion object {
        val CHARSET = Charsets.UTF_8
        val SEPARATOR = ",".toByteArray(CHARSET)
        val LINE_SEPARATOR = "\n".toByteArray(CHARSET)
    }
}
