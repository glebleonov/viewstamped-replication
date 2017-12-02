package scpd.vr.replica

import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.collections.LinkedHashMap

class TestNetwork {
  private var maxAddress = 0
  private val inProgressMap = LinkedHashMap<Address, MutableMap<Address, MutableList<Message>>>()
  private val inProgressList = ArrayDeque<Triple<Address, Address, Message>>()
  private val failed = HashSet<Address>()
  
  private val clientAddresses = HashMap<String, Address>()
  
  private val dispatchers = HashMap<Address, Dispatcher>()

  fun register(address: Address, dispatcher: Dispatcher) {
    dispatchers[address] = dispatcher
  }

  fun nextAddress(): Address {
    return Address(maxAddress++)
  }

  fun nextClientAddress(clientId: String): Address {
    if (clientAddresses.containsKey(clientId)) {
      throw IllegalStateException()
    }

    val address = Address(maxAddress++)
    clientAddresses[clientId] = address
    return address
  }

  fun getClientAddress(clientId: String): Address {
    return clientAddresses[clientId]!!
  }
  
  fun send(from: Address, to: Address, message: Message) {
    if (!(from is Address) || !(to is Address)) {
      throw IllegalStateException()
    }
    inProgressList.add(Triple(from, to, message))
    inProgressMap.computeIfAbsent(from, { address -> LinkedHashMap() }).computeIfAbsent(to, { address -> ArrayList() }).add(message)
    //println("sent: from " + from.number + " to " + to.number + " m " + message)
  }

  private fun receiveNext() {
    val (from, to, message) = inProgressList.removeFirst()
    inProgressMap[from]!![to]!!.remove(message)
    dispatch(from, to, message)
  }

  private fun dispatch(from: Address, to: Address, message: Message) {
    if (failed.contains(from) || failed.contains(to)) {
      return
    }
    
    dispatchers[to]!!.dispatch(message)
  }

  fun receiveNext(from: HasAddress, to: HasAddress) {
    receiveNext(from.getAddress(), to.getAddress())
  }

  private fun receiveNext(from: Address, to: Address) {
    if (!(from is Address) || !(to is Address)) {
      throw IllegalStateException()
    }
    removeFirstFromProgressList(from, to)
    val message = inProgressMap[from]!![to]!!.removeAt(0)
    dispatch(from, to, message)
  }

  fun removeFirstFromProgressList(from: Address, to: Address) {
    var toRemove: Triple<Address, Address, Message>? = null
    for (item in inProgressList) {
      if (item.first.equals(from) && item.second.equals(to)) {
        toRemove = item
        break
      }
    }
    if (toRemove == null) {
      throw IllegalStateException()
    }
    inProgressList.remove(toRemove)
  }

  fun flush() {
    while (!inProgressList.isEmpty()) {
      receiveNext()
    }
  }

  fun isEmpty(): Boolean {
    return inProgressList.isEmpty()
  }

  fun receiveNext(random: Random) {
    val next = next(random)
    receiveNext(next.first, next.second)
  }

  fun next(random: Random): Pair<Address, Address> {
    val nonEmptyFrom = ArrayList<Pair<Address, List<Address>>>()
    for ((from, messages) in inProgressMap) {
      val toList = nonEmptyTo(messages)
      if (!toList.isEmpty()) {
        nonEmptyFrom.add(Pair(from, toList))
      }
    }
    val fromIndex = random.nextInt(nonEmptyFrom.size)
    val toIndex = random.nextInt(nonEmptyFrom[fromIndex].second.size)

    return Pair(nonEmptyFrom[fromIndex].first, nonEmptyFrom[fromIndex].second[toIndex])
  }

  fun nonEmptyTo(map: Map<Address, List<Message>>): List<Address> {
    val res = ArrayList<Address>()
    for ((address, list) in map.entries) {
      if (!list.isEmpty()) {
        res.add(address)
      }
    }
    return res
  }

  fun failed(address: Address) {
    dispatchers.remove(address)
    failed.add(address)
  }

  fun recover(address: Address) {
    failed.remove(address)
  }
}

data class Address(val number: Int)
