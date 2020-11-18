package com.lsmt.core

import com.lsmt.cache.LRUPreCache
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import java.util.*

class LRUPreCacheSpec : StringSpec({
    "evict when full" {
        val underlying = TreeMap<String, String>()
        underlying["0"] = "0"
        underlying["5"] = "5"
        val lru = LRUPreCache(1, underlying)

        lru["0"] shouldBe "0"
        lru["5"] shouldBe "5"
        lru["6"] shouldBe null
        lru.lowerKey("1") shouldBe "0"
        lru.floorKey("0") shouldBe "0"

        lru["1"] = "1"
        lru["1"] shouldBe "1"
        lru.floorKey("1") shouldBe "1"

        lru["2"] = "2"
        lru["1"] shouldBe null
        lru["2"] shouldBe "2"

        lru.size shouldBe 1
    }
})
