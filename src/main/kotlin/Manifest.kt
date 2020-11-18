import com.dslplatform.json.DslJson
import java.io.BufferedOutputStream
import java.io.File
import java.io.OutputStream
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

/**
 * The manifest file contains the state of the levels. We need to keep track of all the SSTable files, and which level
 * they are in.
 */
interface Manifest {
    fun tables(level: Int): List<SSTableMetadata>

    fun addTable(table: SSTableMetadata)

    fun removeTable(table: SSTableMetadata)

    fun apply(operation: Operation)
}

data class KeyRange(
    val first: String,
    val last: String
)

data class SSTableMetadata(
    val name: String,
    val keyRange: KeyRange,
    val level: Int
)

sealed class Operation {
    companion object {
        data class Create(val tables: HashMap<Int, MutableList<SSTableMetadata>>) : Operation()
        data class Add(val table: SSTableMetadata) : Operation()
        data class Remove(val table: SSTableMetadata) : Operation()

        val dslJson = DslJson<Any>()

        fun deserialize(string: String): Operation? =
            when (string.first()) {
                '0' -> dslJson.deserialize(
                    HashMap<Int, MutableList<SSTableMetadata>>().javaClass,
                    string.byteInputStream()
                )?.run { Create(this) }
                '1' -> dslJson.deserialize(SSTableMetadata::class.java, string.byteInputStream())?.run { Add(this) }
                '2' -> dslJson.deserialize(SSTableMetadata::class.java, string.byteInputStream())?.run { Remove(this) }
                else -> null
            }
    }

    fun serialize(outputStream: OutputStream) {
        when (this) {
            is Create -> outputStream.write("0".toByteArray())
            is Add -> outputStream.write("1".toByteArray())
            is Remove -> outputStream.write("2".toByteArray())
        }
        dslJson.serialize(this, outputStream)
    }
}

/**
 * Lists the set of SSTables that make up each level, their key ranges, and other metadata. A new manifest is created
 * whenever the engine restarts.
 *
 * At startup, we read the old manifest file to refresh the in-memory state, then start a new manifest file.
 *
 * The first line of the file includes the starting state. All subsequent lines represent modifications to the state.
 *
 * Periodically, rotate the manifest for faster startup next time.
 */
class StandardManifest(private val rootDirectory: Path) : Manifest, LSMRunnable {
    private lateinit var bos: BufferedOutputStream
    private lateinit var allTables: MutableMap<Int, MutableList<SSTableMetadata>>
    private val rootDirectoryFile = rootDirectory.toFile()
    private val currentFileId = AtomicInteger(largestManifestID())

    override fun tables(level: Int): List<SSTableMetadata> = allTables[level] ?: emptyList()

    override fun addTable(table: SSTableMetadata) {
        Operation.Companion.Add(table).serialize(bos)
        bos.flush()
    }

    override fun removeTable(table: SSTableMetadata) {
        Operation.Companion.Remove(table).serialize(bos)
        bos.flush()
    }

    override fun apply(operation: Operation) {
        when (operation) {
            is Operation.Companion.Create -> allTables = operation.tables.withDefault { ArrayList() }
            is Operation.Companion.Add -> allTables[operation.table.level]?.add(operation.table)
            is Operation.Companion.Remove -> allTables[operation.table.level]?.remove(operation.table)
        }
    }

    override fun start() {
        // Read state from old manifest
        // Create a new manifest. If a manifest already exists, load it into memory.
        val largestManifest = currentFileId.get()
        if (largestManifest >= 0) {
            manifestFile(largestManifest).toFile().forEachLine { line ->
                Operation.deserialize(line)?.also { apply(it) }
            }
        }

        // Save state to new manifest
        nextManifestFile().apply {
            createNewFile()
            outputStream().buffered().use {
                dslJson.serialize(allTables, it)
            }
        }

        // Start accepting writes
        bos = nextManifestFile().outputStream().buffered()
    }

    override fun stop() {
        bos.close()
    }

    private fun manifestFile(id: Int): Path = rootDirectory.resolve(PREFIX + id)

    private fun nextManifestFile(): File = synchronized(this) {
        val id = currentFileId.incrementAndGet()
        return File(rootDirectoryFile, PREFIX + id)
    }

    private fun largestManifestID(): Int {
        var max = -1
        rootDirectoryFile.list { _, name ->
            name?.startsWith(PREFIX) ?: false
        }?.forEach {
            val num = it.removePrefix(PREFIX).toInt()
            max = max(max, num)
        }

        return max
    }

    companion object {
        // File names follow the pattern sstable_1234, where files with larger suffixes were created later.
        const val PREFIX: String = "manifest_"
        val dslJson = DslJson<Any>()
    }
}
