package scpd.vr.replica

import org.junit.Test


abstract class BackupRecoveryTestCase {
  protected val env = TestEnvironment(3)
  private val client = env.createClient("client")

  @Test
  fun recoverDuringRequest() {
    client.send("key", 10)

    failAndRecover()

    afterRecover(client)
  }

  abstract fun failAndRecover()

  @Test
  fun recoverAfterTwoPrepare() {
    client.send("key", 10)

    // client request
    env.network.receiveNext(client, env.replicas[0])
    //prepare messages
    env.network.receiveNext(env.replicas[0], env.replicas[1])
    env.network.receiveNext(env.replicas[0], env.replicas[2])

    failAndRecover()

    afterRecover(client)
  }

  @Test
  fun recoverAfterOnePrepare() {
    client.send("key", 10)

    // client request
    env.network.receiveNext(client, env.replicas[0])
    //prepare messages
    env.network.receiveNext(env.replicas[0], env.replicas[1])

    failAndRecover()

    afterRecover(client)
  }

  protected fun fail(replicaNumber: Int) {
    env.network.failed(env.replicas[replicaNumber].getAddress())
  }

  protected fun recover(replicaNumber: Int) {
    val recoveredReplica = TestReplica(env.network, env.addresses, replicaNumber, TestScheduler(), Status.RECOVERING)
    env.replicas[replicaNumber] = recoveredReplica
    env.network.register(env.addresses[replicaNumber], recoveredReplica)
    env.network.recover(env.addresses[replicaNumber])
    recoveredReplica.recover()
  }

  private fun afterRecover(client: TestClient, expectedValue: Int = 30) {
    client.send("key", 20)

    env.network.flush()

    val expected = mapOf(Pair("key", expectedValue))
    env.assertState(client, expected)
    env.assertState(env.replicas[0], expected)

    env.awaitAllCommitted()
    env.assertReplicasState(expected)
  }
}

class FlushFailFlushRecoverFlush : BackupRecoveryTestCase() {
  override fun failAndRecover() {
    env.network.flush()
    fail(1)
    env.network.flush()
    recover(1)
    env.network.flush()
  }
}

class FailFlushRecoverFlush : BackupRecoveryTestCase() {
  override fun failAndRecover() {
    fail(1)
    env.network.flush()
    recover(1)
    env.network.flush()
  }
}

class FailRecoverFlush : BackupRecoveryTestCase() {
  override fun failAndRecover() {
    fail(1)
    recover(1)
    env.network.flush()
  }
}

class FailFlushRecover : BackupRecoveryTestCase() {
  override fun failAndRecover() {
    fail(1)
    env.network.flush()
    recover(1)
  }
}