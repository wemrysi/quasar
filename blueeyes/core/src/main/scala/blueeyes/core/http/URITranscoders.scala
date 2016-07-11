package blueeyes
package core.http

import blueeyes.util.SpecialCharToStringTranscoder

object URITranscoders {
  final val SafePathChars  = "-_.!~*'()@:$&,;=/"
  final val SafeQueryChars = "-_.!~*'()@:$,;/?:&="

  lazy val pathTranscoder  = transcoder(SafePathChars)
  lazy val queryTranscoder = transcoder(SafeQueryChars)

  private def isOk(c: Char) = (
       (c >= '0' && c <= '9')
    || (c >= 'a' && c <= 'z')
    || (c >= 'A' && c <= 'Z')
    || SafePathChars.indexOf(c) != -1
  )

  private lazy val encoding: PartialFunction[Char, String] = {
    case c: Char if !isOk(c) => java.net.URLEncoder.encode(new String(Array(c)), "UTF-8")
  }
  private lazy val decoding: PartialFunction[List[Char], Option[Char]] = {
    case c => None
  }
  private class CSTranscoder extends SpecialCharToStringTranscoder(encoding, decoding) {
    override def decode(s: String) = java.net.URLDecoder.decode(s, "UTF-8")
  }

  private def transcoder(safeChars: String): SpecialCharToStringTranscoder = new CSTranscoder
}
