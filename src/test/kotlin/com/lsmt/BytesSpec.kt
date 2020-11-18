package com.lsmt

import com.lsmt.toByteArray
import com.lsmt.toDouble
import com.lsmt.toFloat
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class BytesSpec : StringSpec({
    "Float to and from bytes" {
        1.234F.toByteArray().toFloat() shouldBe 1.234F
    }

    "Double to and from bytes" {
        1.234.toByteArray().toDouble() shouldBe 1.234
    }
})
