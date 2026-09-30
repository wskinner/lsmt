package com.lsmt.core

class Config(
    val maxWalSize: Int = 4_000_000,
    val sstablePrefix: String = "sstable_",
    val maxSstableSize: Int = 2_000_000,
    val walPrefix: String = "wal_",
    val maxYoungTables: Int = 4,
    val maxCacheSizeMB: Int = 500,
    val maxActiveTableCreationTasks: Int = 2
)

val DefaultConfig = Config()
