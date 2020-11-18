import java.io.File
import java.util.*

fun main() {
    val tree = StandardLogStructuredMergeTree(
        StandardMemTable(
            TreeMap()
        ),
        StandardSSTableManager(),
        StandardWriteAheadLogManager(
            File("wal.txt")
        )
    )

    tree.put("person1", mapOf(
        "name" to "will",
        "age" to 29
    ))
}
