package scpd.vr.sample.replica

import scpd.vr.network.Channel
import scpd.vr.replica.Message
import scpd.vr.replica.Replica
import scpd.vr.sample.shared.*
import scpd.vr.scheduling.Scheduler
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.net.*
import java.util.concurrent.ScheduledThreadPoolExecutor

class SampleReplica(val socket: DatagramSocket, configuration: List<Channel>, replicaNumber: Int, scheduler: Scheduler):
    Replica<Request, Result>(configuration, replicaNumber, scheduler) {
  val state = State()
  val idToAddress = HashMap<String, SocketAddress>()

  fun register(clientId: String, address: SocketAddress) {
    idToAddress[clientId] = address
  }

  override fun sendResponse(clientId: String, message: Message) {
    send(socket, idToAddress[clientId]!!, message)
  }

  override fun execute(request: Request): Result {
    return state.execute(request)
  }
}

fun main(args : Array<String>) {
  val service = ScheduledThreadPoolExecutor(1)
  val scheduler = ExecutorScheduler(service)

  val replicaNumber = args[0].toInt()
  val addresses = getSocketAddresses(args.takeLast(args.size - 1))
  val selfAddress = addresses[replicaNumber]
  val socket = DatagramSocket(selfAddress)
  val channels = getChannels(socket, addresses, scheduler)
  
  val replica = SampleReplica(socket, channels, replicaNumber, scheduler)

  while (true) {
    receive(socket, scheduler, { m, address ->
      if (m is scpd.vr.replica.Request<*>) {
        replica.register(m.clientId, address)
      }
      replica.receive(m)
    })
  }
}