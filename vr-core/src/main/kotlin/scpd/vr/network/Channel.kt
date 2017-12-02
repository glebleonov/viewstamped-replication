package scpd.vr.network

import scpd.vr.replica.Ack
import scpd.vr.replica.Commit
import scpd.vr.replica.Message
import scpd.vr.scheduling.Scheduler


val DEFAULT_REQUEST_TIMEOUT = 500L

public abstract class Channel(private val scheduler: Scheduler, private val timeoutMs: Long = DEFAULT_REQUEST_TIMEOUT) {
  private val inProgress = HashMap<String, Message>()

  public abstract fun doSend(message: Message)

  fun send(message: Message) {
    doSend(message)

    if (message is Ack || message is Commit) {
      return
    }

    inProgress[message.id] = message
    scheduler.schedule(timeoutMs, {
      if (inProgress.containsKey(message.id)) {
        send(message)
      }
    })
  }

  fun acknowledged(messageId: String) {
    inProgress.remove(messageId)
  }

  fun isEmpty() = inProgress.isEmpty()
}