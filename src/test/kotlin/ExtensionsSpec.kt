import com.lsmt.table.SSTableMetadata
import com.lsmt.table.TableCache
import com.lsmt.table.merge
import com.lsmt.toByteArray
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
            "0" to 0.toByteArray(),
            "1" to 1.toByteArray()
        )

        val table1 = mockk<SSTableMetadata>()
        every { table1.id } returns 1
        every { tableCache.read(table1) } returns sequenceOf(
            "0" to 0.toByteArray(),
            "4" to 4.toByteArray()
        )

        val table2 = mockk<SSTableMetadata>()
        every { table2.id } returns 2
        every { tableCache.read(table2) } returns sequenceOf(
            "2" to 2.toByteArray(),
            "3" to 3.toByteArray()
        )

        val expectedSequence = sequenceOf(
            "0" to 0.toByteArray(),
            "1" to 1.toByteArray(),
            "2" to 2.toByteArray(),
            "3" to 3.toByteArray(),
            "4" to 4.toByteArray()
        )

        val tables = listOf(table0, table1, table2)

        merge(tables, tableCache).toList().zip(expectedSequence.toList()).forEach {
            it.first.second shouldBe it.second.second
        }
    }
})
