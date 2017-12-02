package scpd.vr.sample.shared

import java.io.Serializable

interface Request: Serializable

interface Result: Serializable

data class Increment(val key: String, val value: Int): Request

data class Get(val key: String): Request

data class Success(val value: Int?): Result

data class Error(val error: ErrorCode): Result

enum class ErrorCode {
  BAD_REQUEST
}

public class State {
  val state = HashMap<String, Int>()

  fun execute(request: Request): Result {
    return when (request) {
      is Increment -> {
        val value = state.compute(request.key, { key: String, value: Int? -> (value ?: 0) + request.value })
        println("update " + request.key + " to " + value)
        Success(value)
      }
      is Get -> Success(state.get(request.key))
      else -> Error(ErrorCode.BAD_REQUEST)
    }
  }
}
