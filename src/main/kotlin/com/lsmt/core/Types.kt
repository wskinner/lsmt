package com.lsmt.core

import java.nio.file.Path
import java.util.*

typealias Compactor = Runnable
typealias Record = Map<String, Any>
typealias Entry = Pair<String, Record?>
typealias LevelIndex = SortedMap<Int, Level>
typealias NumberedFile = Pair<Int, Path>

class KeyRange(override val start: String, override val endInclusive: String) : ClosedRange<String>
