package com.lsmt.core

import java.io.File
import java.nio.file.Path
import java.util.zip.CRC32C
import kotlin.math.pow

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

fun CRC32C.checksum(type: Int, data: ByteArray, offset: Int = 0, length: Int = data.size): Int {
    reset()
    update(type)
    update(data, offset, length)
    return value.toInt()
}
