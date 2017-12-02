package scpd.vr.replica

import java.io.Serializable
import java.util.*

abstract class Message(open val id: String = UUID.randomUUID().toString()): Serializable

abstract class ReplicaMessage(
    open val replicaNumber: Int,
    override val id: String = UUID.randomUUID().toString()): Message(id)

data class Request<RequestT: Serializable> (
    val request: RequestT,
    val clientId: String,
    val requestNumber: Int): Message()

data class Reply<ResponseT: Serializable> (
    val viewNumber: Int,
    val requestNumber: Int,
    val response: ResponseT): Message()

data class Obsolete (
    val viewNumber: Int,
    val requestNumber: Int): Message()

data class Prepare<RequestT: Serializable> (
    val viewNumber: Int,
    val request: Request<RequestT>,
    val opNumber: Int,
    val commitNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class PrepareOk (
    val viewNumber: Int,
    val opNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class Commit (
    val viewNumber: Int,
    val commitNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class StartViewChange (
    val viewNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class DoViewChange<RequestT: Serializable> (
    val viewNumber: Int,
    val log: List<Request<RequestT>>,
    val normalViewNumber: Int,
    val opNumber: Int,
    val commitNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class StartView<RequestT: Serializable> (
    val viewNumber: Int,
    val log: List<Request<RequestT>>,
    val opNumber: Int,
    val commitNumber: Int,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber)

data class Recovery (
    override val replicaNumber: Int,
    val uid: String): ReplicaMessage(replicaNumber)

data class RecoveryResponse<RequestT: Serializable> (
    val viewNumber: Int,
    val uid: String,
    override val replicaNumber: Int,
    val log: List<Request<RequestT>>?,
    val opNumber: Int?,
    val commitNumber: Int?): ReplicaMessage(replicaNumber)

data class Ack(
    override val id: String,
    override val replicaNumber: Int): ReplicaMessage(replicaNumber, id)