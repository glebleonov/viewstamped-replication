package scpd.vr.replica

import scpd.vr.network.Channel
import scpd.vr.scheduling.Scheduler
import java.io.Serializable
import java.util.function.Consumer

data class TestRequest(val key: String, val increment: Int): Serializable

data class TestResponse(val key: String, val result: Int): Serializable

interface Dispatcher {
  fun dispatch(message: Message)
}

interface HasState {
  fun getState(): Map<String, Int>
}

class TestScheduler: Scheduler {
  var curTime = 0L;
  val tasks = HashMap<Long, MutableList<() -> Unit>>()

  override fun schedule(delayMillis: Long, task: () -> Unit) {
    tasks.computeIfAbsent(curTime + delayMillis, {time -> ArrayList()}).add(task)
  }

  override fun currentTime(): Long {
    return curTime
  }

  fun addTime(time: Long) {
    for (i in 0..time) {
      incTime()
    }
  }

  private fun incTime() {
    val executed = ArrayList<Long>()
    for ((time, taskList) in HashMap(tasks)) {
      if (time == curTime) {
        for (task in taskList) {
          task()
        }
        executed.add(time)
      }
    }
    for (time in executed) {
      tasks.remove(time)
    }
    curTime++
  }
}

class TestChannel(private val network: TestNetwork, private val source: Address, val destination: Address, scheduler: Scheduler) : Channel(scheduler) {
  override fun doSend(message: Message) {
    network.send(source, destination, message)
  }
}

private fun channels(network: TestNetwork, addresses: List<Address>, replicaNumber: Int, scheduler: Scheduler): List<Channel> {
  return addresses.map { address -> TestChannel(network, addresses[replicaNumber], address, scheduler) }
}

class TestReplica(val network: TestNetwork, configuration: List<Address>, replicaNumber: Int, testScheduler: TestScheduler, status: Status = Status.NORMAL):
    Replica<TestRequest, TestResponse>(channels(network, configuration, replicaNumber, testScheduler), replicaNumber, testScheduler, status), Dispatcher, HasState, HasAddress {

  private val state = State()

  private val testScheduler: TestScheduler

  init {
    network.register(getAddress(), this)
    this.testScheduler = testScheduler
  }

  fun addTime(time: Long) {
    testScheduler.addTime(time)
  }

  override fun getAddress(): Address {
    return (configuration[replicaNumber] as TestChannel).destination as Address
  }

  override fun sendResponse(clientId: String, message: Message) {
    network.send(getAddress(), network.getClientAddress(clientId), message)
  }

  override fun execute(request: TestRequest): TestResponse {
    return state.execute(request)
  }

  override fun getState(): Map<String, Int> {
    return HashMap(state.map)
  }
  
  override fun dispatch(message: Message) {
    receive(message)
  }
}

class TestClient(val network: TestNetwork, id: String, configuration: List<Address>, scheduler: TestScheduler, val ownAddress: Address):
    Client<TestRequest, TestResponse>(configuration.map { address -> TestChannel(network, ownAddress, address, scheduler) }, scheduler, id), Dispatcher, HasState, HasAddress {
  var state = State()

  fun send(key: String, increment: Int) {
    request(TestRequest(key, increment), Consumer { response -> state.execute(response) })
    (scheduler as TestScheduler).addTime(0)
  }

  fun addTime(time: Long) {
    (scheduler as TestScheduler).addTime(time)
  }
  
  override fun getAddress(): Address {
    return ownAddress
  }

  override fun dispatch(message: Message) {
    receive(message)
  }

  override fun getState(): Map<String, Int> {
    return HashMap(state.map)
  }

  init {
    network.register(ownAddress, this)
  }
}

class State {
  val map = HashMap<String, Int>()

  fun execute(request: TestRequest): TestResponse {
    return TestResponse(request.key, map.compute(request.key, { key, value ->
      if (value == null) {
        request.increment
      } else {
        value + request.increment
      }
    })!!)
  }

  fun execute(response: TestResponse) {
    map[response.key] = response.result
  }
}

interface HasAddress {
  fun getAddress(): Address
}
