import com.lsmt.table.SSTableMetadata
import com.lsmt.table.TableCache
import com.lsmt.table.merge
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import io.mockk.every
import io.mockk.mockk

class ExtensionsSpec : StringSpec({

    /**
     * In this test, the element from table1 with key "0" should replace the element with the same key from
     * table0, because table1 has a larger ID, indicating it is older.
     */
    "merge" {
        val tableCache = mockk<TableCache>()
        val table0 = mockk<SSTableMetadata>()
        every { table0.id } returns 0
        every { tableCache.read(table0) } returns sequenceOf(
            "0" to mapOf("0" to 0),
            "1" to mapOf("1" to 1)
        )

        val table1 = mockk<SSTableMetadata>()
        every { table1.id } returns 1
        every { tableCache.read(table1) } returns sequenceOf(
            "0" to mapOf("0" to 1000),
            "4" to mapOf("4" to 4)
        )

        val table2 = mockk<SSTableMetadata>()
        every { table2.id } returns 2
        every { tableCache.read(table2) } returns sequenceOf(
            "2" to mapOf("2" to 2),
            "3" to mapOf("3" to 3)
        )

        val expectedSequence = sequenceOf(
            "0" to mapOf("0" to 1000),
            "1" to mapOf("1" to 1),
            "2" to mapOf("2" to 2),
            "3" to mapOf("3" to 3),
            "4" to mapOf("4" to 4)
        )

        val tables = listOf(table0, table1, table2)

        merge(tables, tableCache).toList() shouldBe expectedSequence.toList()
    }
})
