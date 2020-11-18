interface LSMRunnable {

    // Start the system. If previous failure is detected, attempt restore.
    fun start()

    // Gracefully stop the system.
    fun stop()
}
