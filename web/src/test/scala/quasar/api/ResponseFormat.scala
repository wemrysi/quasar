package quasar.api

import quasar.Predef._

class ResponseFormatSpecs extends org.specs2.mutable.Specification {
  import org.http4s._, QValue._
  import org.http4s.headers.{Accept}

  import MessageFormat._
  import JsonPrecision._
  import JsonFormat._

  "fromAccept" should {
    "use default if no accept provided" in {
      fromAccept(None) must_== MessageFormat.Default
    }

    "choose precise" in {
      val accept = Accept(
        new MediaType("application", "ldjson").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, LineDelimited)
    }

    "choose streaming via boundary extension" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonContentType(Readable, LineDelimited)
    }

    "choose precise list" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, SingleArray)
    }

    "choose streaming and precise via extensions" in {
      val accept = Accept(
        new MediaType("application", "json").withExtensions(Map("mode" -> "precise", "boundary" -> "NL")))
      fromAccept(Some(accept)) must_== JsonContentType(Precise, LineDelimited)
    }

    "choose CSV" in {
      val accept = Accept(
        new MediaType("text", "csv"))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose CSV with custom format" in {
      val accept = Accept(
        new MediaType("text", "csv").withExtensions(Map(
          "columnDelimiter" -> "\t",
          "rowDelimiter" -> ";",
          "quoteChar" -> "'",
          "escapeChar" -> "\\")))
      fromAccept(Some(accept)) must_== Csv('\t', ";", '\'', '\\', None)
    }

    "choose CSV over JSON" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(1.0)),
        new MediaType("application", "ldjson").withQValue(q(0.9)))
      fromAccept(Some(accept)) must_== Csv.Default
    }

    "choose JSON over CSV" in {
      val accept = Accept(
        new MediaType("text", "csv").withQValue(q(0.9)),
        new MediaType("application", "ldjson"))
      fromAccept(Some(accept)) must_== JsonContentType(Readable, LineDelimited)
    }
  }

  "Csv.escapeNewlines" should {
    """escape \r\n""" in {
      Csv.escapeNewlines("\r\n") must_== """\r\n"""
    }

    """not affect \"""" in {
      Csv.escapeNewlines("\\\"") must_== "\\\""
    }
  }

  "Csv.unescapeNewlines" should {
    """unescape \r\n""" in {
      Csv.unescapeNewlines("""\r\n""") must_== "\r\n"
    }

    """not affect \"""" in {
      Csv.escapeNewlines("""\"""") must_== """\""""
    }
  }
}
