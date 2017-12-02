package scpd.vr.scheduling


interface Scheduler {
  fun schedule(delayMillis: Long, task: () -> Unit)

  fun currentTime(): Long
}