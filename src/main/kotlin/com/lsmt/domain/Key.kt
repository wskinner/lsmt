package com.lsmt.domain

import com.lsmt.compareTo

// TODO(will) figure out how to make this an inline class. At time of writing, the overridden methods cause compile
// errors.
data class Key(val byteArray: UByteArray) : Comparable<Key> {
    override fun compareTo(other: Key): Int = byteArray.compareTo(other.byteArray)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Key

        if (!byteArray.contentEquals(other.byteArray)) return false

        return true
    }

    override fun hashCode(): Int {
        return byteArray.contentHashCode()
    }

    override fun toString(): String {
        val contents = byteArray.joinToString(",") { it.toString() }
        return "Key[${contents}]"
    }

    val size = byteArray.size
}
