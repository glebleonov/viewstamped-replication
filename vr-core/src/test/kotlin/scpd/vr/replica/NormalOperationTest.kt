package scpd.vr.replica

import org.junit.Test
import java.util.*


class NormalOperationTest {
  private val env = TestEnvironment(3)

  @Test
  fun simpleChange() {
    val client = env.createClient("client")

    client.send("key", 10)

    env.network.flush()

    val expected = mapOf(Pair("key", 10))
    env.assertState(client, expected)
    env.assertState(env.replicas[0], expected)

    env.awaitAllCommitted()
    env.assertReplicasState(expected)
    env.assertEverythingAcked()
  }

  @Test
  fun random() {
    val random = Random(23917)

    val clients = ArrayList<TestClient>()
    val clientsNumber = 10
    for (i in 0 until clientsNumber) {
      clients.add(env.createClient("client" + i))
    }

    for (client in clients) {
      client.send("key", 10)
    }

    while (!env.network.isEmpty()) {
      env.network.receiveNext(random)
    }

    val expected = mapOf(Pair("key", 10 * clientsNumber))

    env.awaitAllCommitted()
    env.assertReplicasState(expected)
    env.assertEverythingAcked()
  }
}