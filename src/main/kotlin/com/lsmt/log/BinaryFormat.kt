package com.lsmt.log

import com.lsmt.domain.Entry
import com.lsmt.domain.Key
import com.lsmt.domain.Record
import com.lsmt.toByteArray
import com.lsmt.toInt
import com.lsmt.toKey
import java.io.ByteArrayOutputStream
import java.io.InputStream

const val DELETE_MASK = 1 shl 31

/**
 * This binary format is inspired by the LevelDB log format as described in https://github.com/google/leveldb/blob/master/doc/log_format.md
 *
 * Keys are strings. Values are byte arrays.
 *
 * The most significant bit of the key is used to denote deletions.
 *
 * Each key-value pair is encoded as a record.
 * entry := header record
 *
 * header :=
 * delete := boolean
 * key length := uint31
 * key := uint8[key length]
 * value length := uint32
 *
 * record := uint8[value length]
 *
 * @param key string key
 * @param record if null, this operation is a delete
 */
fun encode(key: Key, record: Record?): ByteArray {
    val baos = ByteArrayOutputStream()
    val keyBytes = key.byteArray

    var size = keyBytes.size
    if (record == null) {
        size = size or DELETE_MASK
        baos.write(size.toByteArray())
        baos.write(keyBytes.asByteArray())
    } else {
        baos.write(size.toByteArray())
        baos.write(keyBytes.asByteArray())
        baos.write(record)
    }
    return baos.toByteArray()
}

/**
 * Decode the next KV pair from the stream. If the pair represents a deletion, the second value will be null.
 */
fun InputStream.decode(): Entry {
    val size = readNBytes(4).toInt()
    val isDelete = size < 0
    val keyLength = size and Int.MAX_VALUE
    val key = readNBytes(keyLength).toKey()

    if (isDelete)
        return key to null
    return key to readAllBytes()
}
