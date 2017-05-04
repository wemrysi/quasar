package quasar.precog

package object common {
  type JobId = String
  implicit def stringExtensions(s: String): StringExtensions = new StringExtensions(s)
}

package common {
  final class StringExtensions(s: String) {
    def cpath = CPath(s)
  }
}
