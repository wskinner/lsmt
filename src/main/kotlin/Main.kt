import java.io.File
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayList
import kotlin.concurrent.thread

fun test(times: Int, ops: Int, tree: LogStructuredMergeTree) {
    val runs = ArrayList<Long>()
    for (run in 1..times) {
        val start = System.nanoTime()
        for (i in 1..ops) {
            tree.put(
                "person$i", sortedMapOf(
                    "name" to "will",
                    "age" to 29
                )
            )
        }
        val end = System.nanoTime()
        runs.add(end - start)
    }

    val opsPerSecond = (ops * times).toDouble() / (runs.sum() / 1_000_000_000.0)
    println("Ops per second: $opsPerSecond")
    println(runs)
}

fun main() {
    val tree = StandardLogStructuredMergeTree(
        {
            StandardMemTable(
                TreeMap()
            )
        },
        StandardSSTableManager(
            File("./build/sstables"),
            StandardSerializer(),
            StandardManifest(
                Path.of("./build/sstables/manifest.txt")
            )
        ),
        StandardWriteAheadLogManager(
            File("./build/wal.txt")
        )
    ).apply { start() }

    thread(start = true) {
        test(5, 100_0000, tree)
    }.join()

//    print("Reads")
//    for (i in 1..100000) {
//        tree.get("person$i")
//    }

    tree.stop()
}
