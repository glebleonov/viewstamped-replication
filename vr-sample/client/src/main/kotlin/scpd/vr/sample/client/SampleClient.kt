package scpd.vr.sample.client

import scpd.vr.replica.Client
import scpd.vr.sample.shared.*
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.function.Consumer

fun main(args: Array<String>) {
  val service = ScheduledThreadPoolExecutor(1)
  val scheduler = ExecutorScheduler(service)

  val id = args[0]
  val ss = args[1].split(":")
  val selfAddress = InetSocketAddress(ss[0], ss[1].toInt())

  val addresses = getSocketAddresses(args.takeLast(args.size - 2))
  val socket = DatagramSocket(selfAddress)

  val client = Client<Request, Result>(getChannels(socket, addresses, scheduler), scheduler, id)

  scheduleRequest(scheduler, client)

  while (true) {
    receive(socket, scheduler, {m, address -> client.receive(m) })
  }
}

private fun scheduleRequest(scheduler: ExecutorScheduler, client: Client<Request, Result>) {
  scheduler.schedule(1000, {
    val time = System.currentTimeMillis()
    client.request(Increment("key", 10), Consumer { reply -> println("value " + reply + " got in " + (System.currentTimeMillis() - time) + " ms") })
    scheduleRequest(scheduler, client)
  })
}
