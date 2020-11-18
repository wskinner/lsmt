package com.lsmt.core

import com.lsmt.toByteArray
import com.lsmt.toKey
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class KeySpec : StringSpec({
    "numerics" {
        (0.toByteArray(littleEndian = false).toKey() < 1.toByteArray(littleEndian = false).toKey()) shouldBe true
        0.toByteArray(littleEndian = false).toKey() shouldBe 0.toByteArray(littleEndian = false).toKey()
        (Integer.MAX_VALUE.toByteArray(littleEndian = false)
            .toKey() < (Integer.MAX_VALUE + 1).toByteArray(littleEndian = false).toKey()) shouldBe true
        ((Long.MAX_VALUE - 1).toByteArray(littleEndian = false)
            .toKey() < Long.MAX_VALUE.toByteArray(littleEndian = false).toKey()) shouldBe true
        (Integer.MAX_VALUE.toLong().toByteArray(littleEndian = false)
            .toKey() < Long.MAX_VALUE.toByteArray(littleEndian = false).toKey()) shouldBe true
    }

    "alphabetics" {
        ("a".toByteArray().toKey() < "b".toByteArray().toKey()) shouldBe true
        ("aaaa".toByteArray().toKey() < "b".toByteArray().toKey()) shouldBe true
        ("a".toByteArray().toKey() < "aa".toByteArray().toKey()) shouldBe true
        ("".toByteArray().toKey() < "a".toByteArray().toKey()) shouldBe true
    }
})
