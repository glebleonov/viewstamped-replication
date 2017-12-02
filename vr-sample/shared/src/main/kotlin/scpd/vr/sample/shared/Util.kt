package scpd.vr.sample.shared

import scpd.vr.network.Channel
import scpd.vr.replica.Message
import scpd.vr.scheduling.Scheduler
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

public class UdpChannel(val socket: DatagramSocket, val destination: SocketAddress, scheduler: Scheduler) : Channel(scheduler) {
  override fun doSend(message: Message) {
    send(socket, destination, message)
  }
}

public class ExecutorScheduler(val executor: ScheduledExecutorService): Scheduler {
  override fun schedule(delayMillis: Long, task: () -> Unit) {
    executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS)
  }

  override fun currentTime(): Long {
    return System.currentTimeMillis()
  }
}


public fun send(socket: DatagramSocket, destination: SocketAddress, message: Message) {
  val bos = ByteArrayOutputStream()
  val oos = ObjectOutputStream(bos)
  oos.writeObject(message)
  oos.close()
  val byteArray = bos.toByteArray()
  socket.send(DatagramPacket(byteArray, byteArray.size, destination))
}

public fun receive(socket: DatagramSocket, scheduler: Scheduler, action: (Message, SocketAddress) -> Unit) {
  val buf = ByteArray(100000)
  val packet = DatagramPacket(buf, buf.size)
  socket.receive(packet)
  val b = ByteArrayInputStream(packet.data, 0, packet.length)
  val s = ObjectInputStream(b)
  val m = s.readObject() as Message
  scheduler.schedule(0, { action(m, packet.socketAddress) })
}

public fun getSocketAddresses(args: Iterable<String>): List<SocketAddress> {
  return args.map { s ->
    val ss = s.split(":")
    InetSocketAddress(ss[0], ss[1].toInt())
  }
}

public fun getChannels(socket: DatagramSocket, addresses: Iterable<SocketAddress>, scheduler: Scheduler): List<Channel> {
  return addresses.map { address -> UdpChannel(socket, address, scheduler) }
}