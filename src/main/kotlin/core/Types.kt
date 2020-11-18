package core

import table.SSTableMetadata
import java.util.*

typealias Record = SortedMap<String, Any>
typealias Entry = Pair<String, Record>

// Level -> minKey -> Meta
// The tables are sorted by the minimum key to improve performance of the common table merging operation.
typealias TableIndex = SortedMap<Int, SortedMap<String, SSTableMetadata>>

class KeyRange(override val endInclusive: String, override val start: String) : ClosedRange<String>
