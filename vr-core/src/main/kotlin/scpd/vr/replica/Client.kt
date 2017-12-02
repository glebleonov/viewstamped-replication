package scpd.vr.replica

import scpd.vr.network.Channel
import scpd.vr.network.DEFAULT_REQUEST_TIMEOUT
import scpd.vr.scheduling.Scheduler
import java.io.Serializable
import java.util.function.Consumer
import java.util.logging.Logger

open class Client<RequestT: Serializable, ResultT: Serializable>(val configuration: List<Channel>, val scheduler: Scheduler, val id: String, val requestTimeoutMs: Long = DEFAULT_REQUEST_TIMEOUT) {
  companion object {
    val LOG = Logger.getLogger(Client::class.java.name)
  }

  var requestNumber = 0
  var viewNumber = 0

  private var holder: RequestHolder<RequestT, ResultT>? = null

  fun request(request: RequestT, callback: Consumer<ResultT>) {
    scheduler.schedule(0, {
      if (holder == null) {
        holder = RequestHolder(request, callback, requestNumber)
        send(holder!!)
      } else {
        throw IllegalStateException("previous request isn't completed yet")
      }
    })
  }

  private fun send(requestHolder: RequestHolder<RequestT, ResultT>) {
    getPrimaryChannel().send(Request(requestHolder.request, id, requestNumber))
    scheduleResend(requestHolder)
  }

  private fun scheduleResend(current: RequestHolder<RequestT, ResultT>) {
    scheduler.schedule(requestTimeoutMs, {
      if (holder == current) {
        LOG.info { "failed to get response for $current, broadcasting it" }
        for (channel in configuration) {
          channel.send(Request(current.request, id, requestNumber))
        }
        scheduleResend(current)
      }})
  }

  fun receive(message: Message) {
    when (message) {
      is Obsolete -> doReceive(message)
      is Reply<*> -> doReceive(message as Reply<ResultT>)
      else -> throw IllegalStateException("unknown message type: " + message)
    }
  }

  private fun doReceive(reply: Obsolete) {
    var changed = false

    if (viewNumber != reply.viewNumber) {
      viewNumber = reply.viewNumber
      changed = true
    }

    val oldNumber = requestNumber
    if (reply.requestNumber > requestNumber) {
      requestNumber += reply.requestNumber + 2
      changed = true
    }

    if (changed && holder != null && holder!!.requestNumber == oldNumber) {
      holder!!.requestNumber = requestNumber
      send(holder!!)
    }
  }

  private fun doReceive(reply: Reply<ResultT>) {
    if (viewNumber != reply.viewNumber) {
      viewNumber = reply.viewNumber
    }

    if (holder != null && reply.requestNumber == holder!!.requestNumber) {
      try {
        holder!!.callback.accept(reply.response)
      } finally {
        holder = null
        requestNumber++
      }
    }
  }

  private fun getPrimaryChannel() = configuration[viewNumber % configuration.size]
}

private data class RequestHolder<RequestT: Serializable, ResultT: Serializable>(val request: RequestT, val callback: Consumer<ResultT>, var requestNumber: Int)
