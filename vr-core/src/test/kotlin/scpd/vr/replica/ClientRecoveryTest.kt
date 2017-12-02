package scpd.vr.replica

import org.junit.Test

class ClientRecoveryTest {
  private val env = TestEnvironment(3)
  private val client = env.createClient("client")

  @Test
  fun clientRecover() {
    client.send("key", 10)
    env.network.flush()
    client.send("key", 20)
    env.network.flush()
    
    env.network.failed(client.ownAddress)
    val newClient = env.createClient("client", client.ownAddress)
    env.network.recover(newClient.ownAddress)

    newClient.send("key", 30)
    env.network.flush()
    
    val expected = mapOf(Pair("key", 60))
    env.assertState(newClient, expected)
    env.assertState(env.replicas[0], expected)

    env.awaitAllCommitted()
    env.assertReplicasState(expected)
    env.assertEverythingAcked()
  }
}
