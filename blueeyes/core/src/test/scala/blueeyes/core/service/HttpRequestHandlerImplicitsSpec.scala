package blueeyes
package core.service

import org.specs2.mutable.Specification

class HttpRequestHandlerImplicitsSpec extends Specification with HttpRequestHandlerImplicits {
  "HttpRequestHandlerImplicits.identifierToIdentifierWithDefault: creates IdentifierWithDefault" in {
    val identifierWithDefault = 'foo ?: "bar"
    identifierWithDefault.default mustEqual (Some("bar"))
  }
}
