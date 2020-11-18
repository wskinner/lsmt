package com.lsmt.table

import java.util.*

/**
 * A cache consisting of two levels. The items in the underlying cache will never be evicted. But the items in the
 * LinkedHashMap cache may be evicted when the size limit is reached.
 */
class LRUPreCache<K, V>(
    private val maxSize: Int,
    private val underlying: NavigableMap<K, V>
) : LinkedHashMap<K, V>(
    maxSize * 3,
    0.5F,
    true
), NavigableMap<K, V> by underlying {
    override fun put(key: K, value: V): V? {
        if (!underlying.containsKey(key)) {
            underlying[key] = value
            return super.put(key, value)
        }
        return null
    }

    override fun get(key: K): V? {
        return underlying[key]
    }

    override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean {
        val shouldEvict = size > maxSize
        if (shouldEvict)
            underlying.remove(eldest!!.key)
        return shouldEvict
    }

    override val size: Int
        get() = super.size
}
