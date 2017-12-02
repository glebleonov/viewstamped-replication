package scpd.vr.replica

import org.junit.Test
import scpd.vr.network.DEFAULT_REQUEST_TIMEOUT
import kotlin.test.assertEquals


class ViewChangeTest {
  private val env = TestEnvironment(3)
  private val client = env.createClient("client")

  @Test
  fun simpleViewChange() {
    env.replicas[1].startViewChange()
    env.network.flush()

    afterViewChange(client, 20)
  }

  @Test
  fun singleViewChangeDuringRequest() {
    client.send("key", 10)

    env.replicas[1].startViewChange()
    env.network.flush()

    afterViewChange(client)
  }

  @Test
  fun doubleViewChangeDuringRequest() {
    client.send("key", 10)

    env.replicas[1].startViewChange()
    env.replicas[2].startViewChange()
    env.network.flush()

    afterViewChange(client)
  }

  @Test
  fun viewChangeAfterTwoPrepare() {
    client.send("key", 10)

    // client request
    env.network.receiveNext(client, env.replicas[0])
    //prepare messages
    env.network.receiveNext(env.replicas[0], env.replicas[1])
    env.network.receiveNext(env.replicas[0], env.replicas[2])

    env.replicas[1].startViewChange()
    env.network.flush()

    afterViewChange(client)
  }

  @Test
  fun viewChangeAfterOnePrepare() {
    client.send("key", 10)

    // client request
    env.network.receiveNext(client, env.replicas[0])
    //prepare messages
    env.network.receiveNext(env.replicas[0], env.replicas[1])

    env.replicas[1].startViewChange()
    env.network.flush()

    afterViewChange(client)
  }

  @Test
  fun viewChangeAfterFailure() {
    env.network.failed(env.replicas[0].getAddress())

    env.replicas[1].addTime(2 * COMMIT_MESSAGE_DELAY)
    env.network.flush()

    assertEquals(1, env.replicas[1].viewNumber)
    assertEquals(1, env.replicas[2].viewNumber)
  }

  @Test
  fun startViewChangeByTimeout() {
    client.send("key", 10)

    env.network.failed(env.replicas[0].getAddress())
    env.network.flush()

    env.replicas[1].addTime(2 * COMMIT_MESSAGE_DELAY)
    env.network.flush()

    client.addTime(DEFAULT_REQUEST_TIMEOUT)
    env.network.flush()
    env.awaitAllCommitted()

    val expected = mapOf(Pair("key", 10))
    env.assertState(client, expected)
    env.assertState(env.replicas[1], expected)
    env.assertState(env.replicas[2], expected)
  }

  private fun afterViewChange(client: TestClient, expectedValue: Int = 30) {
    client.send("key", 20)

    env.network.flush()

    val expected = mapOf(Pair("key", expectedValue))
    env.assertState(client, expected)
    env.assertState(env.replicas[1], expected)

    env.awaitAllCommitted()
    env.assertReplicasState(expected)
    env.assertEverythingAcked()
  }
}