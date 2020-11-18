package core

import table.SSTableMetadata
import java.util.*

typealias Record = SortedMap<String, Any>
typealias Entry = Pair<String, Record>
typealias KeyRange = ClosedRange<String>
// Level -> Id -> Meta
typealias TableIndex = SortedMap<Int, SortedMap<Int, SSTableMetadata>>
