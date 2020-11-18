package com.lsmt.core

import com.lsmt.table.SSTableMetadata
import com.lsmt.toSequence
import java.io.File
import java.nio.file.Path
import kotlin.math.pow

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

fun makeFile(rootDirectory: File, prefix: String, id: Int): Path =
    File(
        rootDirectory,
        "$prefix$id"
    ).toPath()

fun min(a: String, b: String): String =
    if (a < b) a
    else b

fun max(a: String, b: String) =
    if (a > b) a
    else b

fun maxLevelSize(level: Int): Int = 10.0.pow(level.toDouble()).toInt()

fun entries(tables: List<SSTableMetadata>): Sequence<Entry> = sequence {
    for (table in tables) {
        yieldAll(table.toSequence())
    }
}
