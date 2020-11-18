package com.lsmt.table

import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

class BlockSpec : StringSpec({
    "string comparisons" {
        ("a" < "b") shouldBe true
        ("aa" < "b") shouldBe true
        ("a" < "aa") shouldBe true
    }
})
