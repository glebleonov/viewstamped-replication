package scpd.vr.replica

import scpd.vr.network.Channel
import scpd.vr.scheduling.Scheduler
import java.io.Serializable
import java.util.*
import java.util.logging.Logger
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.math.max

const val COMMIT_MESSAGE_DELAY: Long = 1000

abstract class Replica<RequestT : Serializable, ResultT : Serializable>
(val configuration: List<Channel>, val replicaNumber: Int, private val scheduler: Scheduler, private var status: Status = Status.NORMAL) {
  companion object {
    val LOG = Logger.getLogger(Client::class.java.name)
  }
  
  var viewNumber = 0
    private set(value) {
      field = value
    }

  private var normalViewNumber = 0
  private var opNumber = -1
  private var commitNumber = -1

  private val uid = UUID.randomUUID().toString()

  private val preparedBackups = HashMap<Int, MutableSet<Int>>()
  private val startedViewChange = HashMap<Int, MutableSet<Int>>()
  private val didViewChange = HashMap<Int, MutableMap<Int, DoViewChange<RequestT>>>()

  private val recovered = HashMap<Int, RecoveryResponse<RequestT>>()

  private var log = ArrayList<Request<RequestT>>()

  private val clientTable = HashMap<String, ClientRequest<ResultT>>()

  private var lastPrimaryRequest = 0L
  
  init {
    scheduleCommitMessage()
    scheduleViewChange()
  }

  fun scheduleViewChange() {
    scheduler.schedule(2 * COMMIT_MESSAGE_DELAY, {
      // strict equality is only for test purposes
      val elapsed = scheduler.currentTime() - lastPrimaryRequest
      if (!isPrimary() && elapsed >= 2 * COMMIT_MESSAGE_DELAY) {
        LOG.warning("$replicaNumber: $elapsed from last primary message, starting view change")
        startViewChange()
      }
      scheduleViewChange()
    })
  }

  fun receive(message: Message) {
    if (message is ReplicaMessage && message.replicaNumber == getPrimaryNumber(viewNumber)) {
      lastPrimaryRequest = scheduler.currentTime()
    }

    when (message) {
      is Request<*> -> doReceive(message as Request<RequestT>)
      is Prepare<*> -> doReceive(message as Prepare<RequestT>)
      is PrepareOk -> doReceive(message)
      is Commit -> doReceive(message)
      is StartViewChange -> doReceive(message)
      is DoViewChange<*> -> doReceive(message as DoViewChange<RequestT>)
      is StartView<*> -> doReceive(message as StartView<RequestT>)
      is Ack -> doReceive(message)
      is Recovery -> doReceive(message)
      is RecoveryResponse<*> -> doReceive(message as RecoveryResponse<RequestT>)
      else -> throw IllegalStateException()
    }
  }

  fun recover() {
    if (status != Status.RECOVERING) {
      throw IllegalStateException()
    }
    broadcast(Recovery(replicaNumber, uid), false)
  }

  fun doReceive(recovery: Recovery) {
    if (status != Status.NORMAL) {
      return
    }
    sendAck(recovery)

    val response: RecoveryResponse<RequestT>
    if (isPrimary()) {
      response = RecoveryResponse(viewNumber, recovery.uid, replicaNumber, log, opNumber, commitNumber)
    } else {
      response = RecoveryResponse(viewNumber, recovery.uid, replicaNumber, null, null, null)
    }
    send(recovery.replicaNumber, response)
  }

  fun doReceive(recoveryResponse: RecoveryResponse<RequestT>) {
    sendAck(recoveryResponse)

    if (status != Status.RECOVERING || !uid.equals(recoveryResponse.uid)) {
      return
    }

    recovered[recoveryResponse.replicaNumber] = recoveryResponse

    if (recovered.size >= consensusThreshold() + 1) {
      tryFinishRecovering()
    }
  }

  private fun tryFinishRecovering() {
    val maxViewNumber = recovered.values.maxBy { response -> response.viewNumber }!!.viewNumber

    val primaryResponse = recovered.values.firstOrNull { response -> response.viewNumber == maxViewNumber && response.log != null }

    if (primaryResponse != null) {
      log = ArrayList(primaryResponse.log!!)
      opNumber = primaryResponse.opNumber!!
      commitAsBackup(primaryResponse.commitNumber!!)

      status = Status.NORMAL
    }
  }

  fun doReceive(ack: Ack) {
    configuration[ack.replicaNumber].acknowledged(ack.id)
  }

  fun doReceive(request: Request<RequestT>) {
    val currentRequest = clientTable[request.clientId]
    val currentRequestNumber = currentRequest?.requestNumber ?: -1

    if (!isPrimary()) {
      sendResponse(request.clientId, Obsolete(viewNumber, currentRequestNumber))
      return
    }

    if (status != Status.NORMAL) {
      return
    }

    if (currentRequest != null) {
      if (currentRequest.requestNumber > request.requestNumber) {
        sendResponse(request.clientId, Obsolete(viewNumber, currentRequestNumber))
        return
      }

      if (currentRequest.requestNumber == request.requestNumber) {
        if (currentRequest.executed()) {
          sendResponse(request.clientId, Reply(viewNumber, currentRequest.requestNumber, currentRequest.result()))
        }
        return
      }
    }

    add(request)

    val prepare = Prepare(viewNumber, request, opNumber, commitNumber, replicaNumber)

    broadcast(prepare, false)
  }

  fun startViewChange() {
    if (isPrimary()) {
      throw IllegalStateException()
    }

    doStartViewChange(viewNumber + 1)
  }

  private fun doStartViewChange(newViewNumber: Int) {
    viewNumber = newViewNumber
    status = Status.VIEW_CHANGE
    broadcast(StartViewChange(viewNumber, replicaNumber), true)
  }

  fun doReceive(startViewChange: StartViewChange) {
    sendAck(startViewChange)

    if (viewNumber < startViewChange.viewNumber) {
      doStartViewChange(startViewChange.viewNumber)
    }

    val started = startedViewChange.computeIfAbsent(startViewChange.viewNumber, { key -> HashSet() })
    started.add(startViewChange.replicaNumber)
    if (started.size == consensusThreshold() + 1) {
      send(getPrimaryNumber(startViewChange.viewNumber), DoViewChange(startViewChange.viewNumber, ArrayList(log), normalViewNumber, opNumber, commitNumber, replicaNumber))
      startedViewChange.remove(startViewChange.viewNumber)
    }
  }

  fun doReceive(doViewChange: DoViewChange<RequestT>) {
    sendAck(doViewChange)

    if (viewNumber < doViewChange.viewNumber) {
      doStartViewChange(doViewChange.viewNumber)
    }

    val changed = didViewChange.computeIfAbsent(doViewChange.viewNumber, { key -> HashMap() })
    changed[doViewChange.replicaNumber] = doViewChange
    if (changed.size == consensusThreshold() + 1) {
      changeView(doViewChange.viewNumber, changed.values)
      didViewChange.remove(doViewChange.viewNumber)
    }
  }

  fun doReceive(startView: StartView<RequestT>) {
    sendAck(startView)

    if (startView.viewNumber < viewNumber) {
      return
    }

    viewNumber = startView.viewNumber
    opNumber = startView.opNumber

    replaceLog(startView.log)

    commitAsBackup(startView.commitNumber)
    for (request in log.subList(commitNumber + 1, log.size)) {
      clientTable.put(request.clientId, ClientRequest(request.requestNumber))
    }
    if (opNumber != commitNumber) {
      send(getPrimaryNumber(viewNumber), PrepareOk(viewNumber, opNumber, replicaNumber))
    }

    status = Status.NORMAL
  }

  private fun replaceLog(newLog: List<Request<RequestT>>) {
    log = ArrayList(newLog)
    for (request in log) {
      updateClientTable(request)
    }
  }

  private fun changeView(viewNumber: Int, changes: Collection<DoViewChange<RequestT>>) {
    if (this.viewNumber != viewNumber) {
      throw IllegalStateException()
    }
    normalViewNumber = viewNumber

    val best = changes.maxWith(Comparator { c1, c2 ->
      if (c1.normalViewNumber != c2.normalViewNumber) {
        compareValues(c1.normalViewNumber, c2.normalViewNumber)
      }
      compareValues(c1.opNumber, c2.opNumber)
    })!!

    replaceLog(best.log)
    opNumber = best.opNumber

    val newCommitNumber = max(commitNumber, changes.maxBy { viewChange -> viewChange.commitNumber }!!.commitNumber)

    commitAsPrimary(newCommitNumber)

    status = Status.NORMAL
    LOG.info("$replicaNumber: view changed to $viewNumber")
    
    broadcast(StartView(viewNumber, log, opNumber, commitNumber, replicaNumber), false)
  }

  private fun broadcast(message: Message, includeSelf: Boolean) {
    for (channel in configuration) {
      if (!includeSelf && channel == configuration[replicaNumber]) {
        continue
      }
      channel.send(message)
    }
  }

  fun doReceive(prepare: Prepare<RequestT>) {
    if (status != Status.NORMAL) {
      return
    }

    if (opNumber + 1 < prepare.opNumber) {
      return
    }

    sendAck(prepare)

    if (!shouldProcess(false, prepare.viewNumber)) {
      return
    }

    if (opNumber + 1 > prepare.opNumber) {
      return
    }

    commitAsBackup(prepare.commitNumber)

    add(prepare.request)

    send(prepare.replicaNumber, PrepareOk(viewNumber, prepare.opNumber, replicaNumber))
  }

  private fun commitAsBackup(newCommitNumber: Int) {
    for (request in log.subList(commitNumber + 1, newCommitNumber + 1)) {
      commit(request)
    }
  }

  fun doReceive(prepareOk: PrepareOk) {
    if (status != Status.NORMAL) {
      return
    }
    sendAck(prepareOk)

    if (!shouldProcess(true, prepareOk.viewNumber)) {
      return
    }

    if (prepareOk.opNumber < commitNumber) {
      return
    }

    val prepared = preparedBackups.computeIfAbsent(prepareOk.opNumber, { key -> HashSet() })
    prepared.add(prepareOk.replicaNumber)
    if (prepared.size == consensusThreshold()) {
      commitAsPrimary(prepareOk.opNumber)
      preparedBackups.remove(prepareOk.opNumber)
    }
  }

  private fun commitAsPrimary(to: Int) {
    for (request in log.subList(commitNumber + 1, to + 1)) {
      val sameRequest = commit(request)
      val clientRequest = clientTable[request.clientId]!!
      if (sameRequest) {
        sendResponse(request.clientId, Reply(viewNumber, clientRequest.requestNumber, clientRequest.result()))
      }
    }
  }

  private fun scheduleCommitMessage() {
    val message = Commit(viewNumber, commitNumber, replicaNumber)
    scheduler.schedule(COMMIT_MESSAGE_DELAY, {
      if (isPrimary() && message.viewNumber == viewNumber && message.commitNumber == commitNumber) {
        broadcast(message, false)
      }
      scheduleCommitMessage()
    })
  }

  fun doReceive(commit: Commit) {
    if (status != Status.NORMAL) {
      return
    }

    if (!shouldProcess(false, commit.viewNumber) || commitNumber >= commit.commitNumber) {
      return
    }

    commitAsBackup(Math.min(commit.commitNumber, opNumber + 1))
  }

  private fun shouldProcess(primary: Boolean, viewNumber: Int): Boolean {
    if (this.viewNumber != viewNumber) {
      return false
    }
    
    if (isPrimary() != primary) {
      return false
    }

    return true
  }

  private fun commit(request: Request<RequestT>): Boolean {
    val result = execute(request.request)
    commitNumber++

    val clientId = request.clientId

    val sameRequest = if (clientTable[clientId] == null) false else clientTable[clientId]!!.requestNumber == request.requestNumber

    updateClientTable(request)

    clientTable[clientId]!!.result = result

    return sameRequest
  }

  private fun updateClientTable(request: Request<RequestT>) {
    val clientId = request.clientId
    if (!clientTable.containsKey(clientId) || clientTable[clientId]!!.requestNumber < request.requestNumber) {
      clientTable[clientId] = ClientRequest(request.requestNumber)
    }
  }

  private fun add(request: Request<RequestT>) {
    opNumber++
    log.add(request)
    clientTable.put(request.clientId, ClientRequest(request.requestNumber))
  }

  private fun isPrimary(): Boolean {
    return getPrimaryNumber(viewNumber) == replicaNumber
  }

  private fun getPrimaryNumber(viewNumber: Int) = viewNumber % configuration.size

  private fun consensusThreshold(): Int {
    return (configuration.size - 1) / 2
  }

  fun sendAck(message: ReplicaMessage) {
    send(message.replicaNumber, Ack(message.id, replicaNumber))
  }

  fun send(destination: Int, message: Message) {
    configuration[destination].send(message)
  }

  abstract fun sendResponse(clientId: String, message: Message)
  abstract fun execute(request: RequestT): ResultT
}

enum class Status {
  NORMAL,
  VIEW_CHANGE,
  RECOVERING
}

class ClientRequest<ResultT>(val requestNumber: Int) {
  var result: ResultT? = null

  fun executed(): Boolean {
    return result != null
  }

  fun result(): ResultT {
    if (!executed()) {
      throw IllegalStateException("request hasn't been executed yet")
    } else {
      return result!!
    }
  }
}