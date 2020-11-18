package core

import java.util.*

typealias Record = SortedMap<String, Any>
typealias Entry = Pair<String, Record>

typealias LevelIndex = SortedMap<Int, Level>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>
