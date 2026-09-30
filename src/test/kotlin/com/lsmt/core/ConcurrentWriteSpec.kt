package com.lsmt.core

import com.lsmt.domain.Key
import com.lsmt.toByteArray
import com.lsmt.toKey
import com.lsmt.treeFactory
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

class ConcurrentWriteSpec : StringSpec({

    "concurrent puts stay visible across memtable rotation" {
        val tree = treeFactory(Config(maxWalSize = 2_048))
        val writers = 4
        val readers = 2
        val perWriter = 200
        val published = ConcurrentHashMap<Key, ByteArray>()
        val errors = AtomicReference<Throwable>(null)
        val start = CountDownLatch(1)
        val writersDone = CountDownLatch(writers)
        val running = AtomicBoolean(true)
        val pool = Executors.newFixedThreadPool(writers + readers)

        try {
            repeat(readers) {
                pool.submit {
                    try {
                        start.await()
                        while (running.get()) {
                            for ((key, value) in published) {
                                val found = tree.get(key)
                                if (found == null || !found.contentEquals(value)) {
                                    errors.compareAndSet(
                                        null,
                                        AssertionError("lost write for $key found=${found?.contentToString()}")
                                    )
                                    return@submit
                                }
                            }
                        }
                    } catch (t: Throwable) {
                        errors.compareAndSet(null, t)
                    }
                }
            }

            repeat(writers) { writer ->
                pool.submit {
                    try {
                        start.await()
                        repeat(perWriter) { i ->
                            val key = key(writer.toLong() * 1_000_000L + i)
                            val value = ByteArray(48) { (writer + i).toByte() }
                            tree.put(key, value)
                            val found = tree.get(key)
                            if (found == null || !found.contentEquals(value)) {
                                errors.compareAndSet(null, AssertionError("writer missed its own put"))
                                return@submit
                            }
                            published[key] = value
                        }
                    } catch (t: Throwable) {
                        errors.compareAndSet(null, t)
                    } finally {
                        writersDone.countDown()
                    }
                }
            }

            start.countDown()
            writersDone.await(120, TimeUnit.SECONDS) shouldBe true
            running.set(false)
            pool.shutdown()
            pool.awaitTermination(120, TimeUnit.SECONDS) shouldBe true
            errors.get()?.let { throw it }

            published.size shouldBe writers * perWriter
            for ((key, value) in published) {
                val found = tree.get(key)
                check(found != null && found.contentEquals(value)) { "missing $key before close" }
            }
            tree.close()
            for ((key, value) in published) {
                val found = tree.get(key)
                check(found != null && found.contentEquals(value)) { "missing $key after close" }
            }
        } finally {
            running.set(false)
            pool.shutdownNow()
            tree.close()
        }
    }

    "concurrent updates of one key are not torn" {
        val tree = treeFactory(Config(maxWalSize = 2_048))
        val key = key(42)
        val threads = 8
        val writes = 40
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        val errors = AtomicReference<Throwable>(null)
        val pool = Executors.newFixedThreadPool(threads)

        try {
            repeat(threads) { thread ->
                pool.submit {
                    try {
                        start.await()
                        val value = ByteArray(64) { thread.toByte() }
                        repeat(writes) {
                            tree.put(key, value)
                        }
                    } catch (t: Throwable) {
                        errors.compareAndSet(null, t)
                    } finally {
                        done.countDown()
                    }
                }
            }

            start.countDown()
            done.await(60, TimeUnit.SECONDS) shouldBe true
            pool.shutdown()
            pool.awaitTermination(30, TimeUnit.SECONDS) shouldBe true
            errors.get()?.let { throw it }

            val found = tree.get(key) ?: error("missing key")
            check(found.all { it == found[0] }) { "torn value ${found.contentToString()}" }
            tree.close()
            val afterClose = tree.get(key) ?: error("missing key after close")
            check(afterClose.contentEquals(found)) { "value changed across flush" }
        } finally {
            pool.shutdownNow()
            tree.close()
        }
    }

    "concurrent put and delete" {
        val tree = treeFactory()
        val threads = 4
        val perThread = 50
        val start = CountDownLatch(1)
        val done = CountDownLatch(threads)
        val errors = AtomicReference<Throwable>(null)

        try {
            repeat(threads) { thread ->
                Thread {
                    try {
                        start.await()
                        repeat(perThread) { i ->
                            val key = key(thread.toLong() * perThread + i)
                            val value = ByteArray(16) { i.toByte() }
                            tree.put(key, value)
                            if (i % 2 == 0) {
                                tree.delete(key)
                            }
                        }
                    } catch (t: Throwable) {
                        errors.compareAndSet(null, t)
                    } finally {
                        done.countDown()
                    }
                }.start()
            }

            start.countDown()
            done.await(60, TimeUnit.SECONDS) shouldBe true
            errors.get()?.let { throw it }

            repeat(threads) { thread ->
                repeat(perThread) { i ->
                    val key = key(thread.toLong() * perThread + i)
                    val found = tree.get(key)
                    if (i % 2 == 0) {
                        check(found == null) { "deleted key still present" }
                    } else {
                        val expected = ByteArray(16) { i.toByte() }
                        check(found != null && found.contentEquals(expected)) { "missing $key" }
                    }
                }
            }
        } finally {
            tree.close()
        }
    }

    "writes fail after close" {
        val tree = treeFactory()
        tree.close()
        shouldThrow<IllegalStateException> {
            tree.put(key(1), byteArrayOf(1))
        }
        shouldThrow<IllegalStateException> {
            tree.delete(key(1))
        }
    }
})

private fun key(value: Long): Key = value.toByteArray(littleEndian = false).toKey()
