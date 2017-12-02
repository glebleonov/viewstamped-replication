package scpd.vr.replica

import org.junit.Test
import scpd.vr.network.DEFAULT_REQUEST_TIMEOUT


class ClientBroadcastTest {
  private val env = TestEnvironment(3)
  private val client = env.createClient("client")

  @Test
  fun broadcast() {
    client.send("key", 10)
    client.addTime(DEFAULT_REQUEST_TIMEOUT)

    env.network.flush()
    env.awaitAllCommitted()
    val expected = mapOf(Pair("key", 10))
    env.assertReplicasState(expected)
    env.assertEverythingAcked()
  }
}