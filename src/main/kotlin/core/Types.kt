package core

import java.nio.file.Path
import java.util.*

typealias Record = SortedMap<String, Any>
typealias Entry = Pair<String, Record?>
typealias SafeEntry = Pair<String, Record>
typealias LevelIndex = SortedMap<Int, Level>
typealias NumberedFile = Pair<Int, Path>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>
