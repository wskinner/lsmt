package com.lsmt.log

import com.lsmt.core.Record

interface LogWriter : AutoCloseable {

    /**
     * Append a record to the log. Return the number of bytes written.
     * Passing a null record signifies a deletion.
     */
    fun append(key: String, value: Record?): Int

    // Total number of bytes written to the file.
    fun size(): Int
}
