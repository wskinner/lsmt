package core

import table.SSTableMetadata
import table.TableKey
import java.util.*

typealias Record = SortedMap<String, Any>
typealias Entry = Pair<String, Record>

// Level -> minKey -> Meta
// The tables are sorted by the minimum key to improve performance of the common table merging operation.
typealias TableIndex = SortedMap<Int, SortedMap<TableKey, SSTableMetadata>>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>
