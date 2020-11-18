package com.lsmt

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class BytesSpec : StringSpec({
    "Float to and from bytes" {
        1.234F.toByteArray().toFloat() shouldBe 1.234F
    }

    "Double to and from bytes" {
        1.234.toByteArray().toDouble() shouldBe 1.234
    }

    "long to and from bytes" {
        1234L.toByteArray().toLong() shouldBe 1234L
        1234L.toByteArray(littleEndian = false).toLong(littleEndian = false) shouldBe 1234L
    }

    "int to and from bytes" {
        1234.toByteArray().toInt() shouldBe 1234
        1234.toByteArray(littleEndian = false).toInt(littleEndian = false) shouldBe 1234
    }
})
