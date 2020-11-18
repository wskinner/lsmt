package com.lsmt

object Config {
    val maxWalSize = 4_000_000
    val sstablePrefix = "sstable_"
    val maxSstableSize = 2_000_000
    val walPrefix = "wal_"
    val maxYoungTables = 4
    val maxCacheSizeMB = 500
    val maxActiveTableCreationTasks = 2
}
