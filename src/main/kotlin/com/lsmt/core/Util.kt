package com.lsmt.core

import com.lsmt.toByteArray
import java.io.File
import java.nio.file.Path
import java.util.zip.CRC32C
import kotlin.math.pow

fun makeFile(rootDirectory: File, prefix: String, id: Long): Path =
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

fun maxLevelSize(level: Int): Int =
    if (level == 0)
        4
    else
        10.0.pow(level.toDouble()).toInt()

fun CRC32C.checksum(
    type: Int,
    data: ByteArray,
    offset: Int = 0,
    length: Int = data.size
): Int {
    reset()
    update(type)
    update(data, offset, length)
    return value.toInt()
}

fun CRC32C.checksum(
    type: Int,
    size: Int,
    key: ByteArray,
    value: ByteArray
): Int {
    reset()
    update(type)
    update(size.toByteArray())
    update(key)
    update(value)
    return this.value.toInt()
}
