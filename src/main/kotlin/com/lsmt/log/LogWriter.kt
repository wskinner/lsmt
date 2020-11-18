package com.lsmt.log

import com.lsmt.core.Key
import com.lsmt.core.Record

interface LogWriter : AutoCloseable {

    /**
     * Append a record to the log. Return the number of bytes written.
     * Passing a null record signifies a deletion.
     */
    fun append(key: Key, value: Record?): Int

    // Total number of bytes written to the file.
    fun size(): Int

    /**
     * Return the number of bytes written since the last rotate() call
     */
    fun totalBytes(): Int
}
