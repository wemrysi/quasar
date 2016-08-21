package ygg.json

import scala.language.experimental.macros
import ygg.json.literal.{ LiteralMacros => LM }

trait JsonMacros {
  val Json        = io.circe.Json
  val Encoder     = io.circe.Encoder
  val Decoder     = io.circe.Decoder
  type Json       = io.circe.Json
  type Encoder[A] = io.circe.Encoder[A]
  type Decoder[A] = io.circe.Decoder[A]
  type HCursor    = io.circe.HCursor

  implicit final def decodeLiteralBoolean[S <: Boolean]: Decoder[S] = macro LM.decodeLiteralBooleanImpl[S]
  implicit final def decodeLiteralChar[S <: Char]: Decoder[S]       = macro LM.decodeLiteralCharImpl[S]
  implicit final def decodeLiteralDouble[S <: Double]: Decoder[S]   = macro LM.decodeLiteralDoubleImpl[S]
  implicit final def decodeLiteralFloat[S <: Float]: Decoder[S]     = macro LM.decodeLiteralFloatImpl[S]
  implicit final def decodeLiteralInt[S <: Int]: Decoder[S]         = macro LM.decodeLiteralIntImpl[S]
  implicit final def decodeLiteralLong[S <: Long]: Decoder[S]       = macro LM.decodeLiteralLongImpl[S]
  implicit final def decodeLiteralString[S <: String]: Decoder[S]   = macro LM.decodeLiteralStringImpl[S]
  implicit final def encodeLiteralBoolean[S <: Boolean]: Encoder[S] = macro LM.encodeLiteralBooleanImpl[S]
  implicit final def encodeLiteralChar[S <: Char]: Encoder[S]       = macro LM.encodeLiteralCharImpl[S]
  implicit final def encodeLiteralDouble[S <: Double]: Encoder[S]   = macro LM.encodeLiteralDoubleImpl[S]
  implicit final def encodeLiteralFloat[S <: Float]: Encoder[S]     = macro LM.encodeLiteralFloatImpl[S]
  implicit final def encodeLiteralInt[S <: Int]: Encoder[S]         = macro LM.encodeLiteralIntImpl[S]
  implicit final def encodeLiteralLong[S <: Long]: Encoder[S]       = macro LM.encodeLiteralLongImpl[S]
  implicit final def encodeLiteralString[S <: String]: Encoder[S]   = macro LM.encodeLiteralStringImpl[S]

  implicit final class JsonStringContext(sc: StringContext) {
    final def json(args: Any*): Json = macro LM.jsonStringContext
  }
}

package object literal extends JsonMacros
