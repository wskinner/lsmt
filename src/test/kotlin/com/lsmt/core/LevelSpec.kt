package com.lsmt.core

import com.lsmt.table.SSTableMetadata
import com.lsmt.toKey
import io.kotlintest.matchers.collections.shouldBeLargerThan
import io.kotlintest.specs.StringSpec

class LevelSpec : StringSpec({
    "get key in range" {
        val level = StandardLevel(0)
        level.add(
            SSTableMetadata(
                "",
                "key0".toByteArray().toKey(),
                "key200000".toByteArray().toKey(),
                0,
                16,
                0
            )
        )

        level.get("key2".toByteArray().toKey()) shouldBeLargerThan emptyList<SSTableMetadata>()
    }
})
