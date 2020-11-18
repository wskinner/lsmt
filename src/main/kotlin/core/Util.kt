package core

import java.io.File

fun nextFile(directory: File, prefix: String): Int {
    val current = currentFile(directory, prefix)
    return current + 1
}

fun currentFile(directory: File, prefix: String): Int =
    directory.list { _, name ->
        name?.startsWith(prefix) ?: false
    }?.map {
        it.removePrefix(prefix).toInt()
    }?.max() ?: 0

fun min(a: String, b: String): String =
    if (a < b) a
    else b

fun max(a: String, b: String) =
    if (a > b) a
    else b
