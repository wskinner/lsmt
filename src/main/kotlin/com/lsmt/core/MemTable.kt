package com.lsmt.core

import com.lsmt.domain.Key
import com.lsmt.domain.Record
import java.util.*

interface MemTable : Iterable<MutableMap.MutableEntry<Key, Record>> {
    fun put(key: Key, value: Record)

    fun get(key: Key): Record?

    fun getRecord(key: Key): Record?

    fun delete(key: Key)

    fun size(): Int

    val storage: SortedMap<Key, Record>
}

class StandardMemTable(
    override val storage: SortedMap<Key, Record>
) : MemTable {

    override fun put(key: Key, value: Record) {
        storage[key] = value
    }

    override fun get(key: Key): Record? = storage[key]

    override fun getRecord(key: Key): Record? = storage[key]

    override fun delete(key: Key) {
        storage.remove(key)
    }

    override fun size() = storage.size

    override fun iterator() = storage.iterator()
}
