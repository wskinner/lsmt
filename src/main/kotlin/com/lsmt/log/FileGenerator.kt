package com.lsmt.log

import com.lsmt.core.NumberedFile
import com.lsmt.core.makeFile
import com.lsmt.core.nextFile
import java.io.File

/**
 * A common pattern is to generate a series of file names which consist of a prefix and a monotonically increasing ID.
 * This interface captures that functionality. Synchronization is performed at the instance level. If you create more
 * than one instance for the same root directory and prefix, race conditions are possible. Only create one instance for
 * each root (directory, prefix) pair and reuse the instance.
 */
interface FileGenerator {
    /**
     * Return the next file and its ID. This function must never return the same file to two different callers.
     */
    fun next(): NumberedFile
}

class SynchronizedFileGenerator(
    private val rootDirectory: File,
    private val prefix: String
) : FileGenerator {
    override fun next(): NumberedFile = synchronized(this) {
        val id = nextFileId()
        return id to makeFile(rootDirectory, prefix, id)
    }

    private fun nextFileId(): Int = nextFile(rootDirectory, prefix)

}
