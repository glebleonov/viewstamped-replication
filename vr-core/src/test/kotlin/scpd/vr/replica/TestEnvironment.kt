package scpd.vr.replica

import java.util.ArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class TestEnvironment(replicasNumber: Int) {
  val network = TestNetwork()

  val addresses = ArrayList<Address>()
  val replicas = ArrayList<TestReplica>()

  init {
    for (i in 0 until replicasNumber) {
      addresses.add(network.nextAddress())
    }

    for (i in 0 until replicasNumber) {
      replicas.add(TestReplica(network, addresses, i, TestScheduler()))
    }
  }

  fun createClient(name: String, address: Address = network.nextClientAddress(name)): TestClient {
    return TestClient(network, name, addresses, TestScheduler(), address)
  }

  fun awaitAllCommitted() {
    for (i in 0..2 * COMMIT_MESSAGE_DELAY) {
      for (replica in replicas) {
        replica.addTime(1)
        network.flush()
      }
    }
  }

  fun assertState(hasState: HasState, expected: Map<String, Int>) {
    assertEquals(expected, hasState.getState())
  }

  fun assertReplicasState(expected: Map<String, Int>) {
    for (replica in replicas) {
      assertState(replica, expected)
    }
  }

  fun assertEverythingAcked() {
    for (replica in replicas) {
      for (channel in replica.configuration) {
        assertTrue((channel as TestChannel).isEmpty())
      }
    }
  }
}