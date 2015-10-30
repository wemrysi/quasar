package quasar
package api
package services

import jawn.{FContext, Facade, AsyncParser}
import org.http4s.server.middleware.GZip
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import quasar.Data
import quasar.Predef._
import quasar.api.MessageFormat._
import JsonPrecision._
import JsonFormat._
import quasar.DataCodec

import argonaut.{JsonObject, JsonNumber, Json}
import argonaut.Argonaut._
import jawnstreamz._
import org.http4s._
import org.http4s.headers._
import org.http4s.server._
import quasar.fs.{Path => _, _}
import pathy.Path
import pathy.Path._
import quasar.fs.PathyGen._
import quasar.fs.NumericGen._

import scala.collection.mutable
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

import org.scalacheck.Arbitrary


class DataServiceSpec extends Specification with ScalaCheck with FileSystemFixture with Http4s {
  import inmemory._, quasar.DataGen._

  def service(mem: InMemState): HttpService =
    data.service[FileSystem](runStatefully(mem).run.compose(fileSystem))

  implicit val arbJson: Arbitrary[Json] = Arbitrary(Arbitrary.arbitrary[String].map(jString(_)))

  val jsonReadableLine = JsonContentType(Readable,LineDelimited)
  val csv = MediaType.`text/csv`

  // See: https://github.com/non/jawn/pull/43
  implicit val bugFreeArgonautFacade: Facade[Json] =
    new Facade[Json] {
      def jnull() = Json.jNull
      def jfalse() = Json.jFalse
      def jtrue() = Json.jTrue
      def jnum(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jint(s: String) = Json.jNumber(JsonNumber.unsafeDecimal(s))
      def jstring(s: String) = Json.jString(s)

      def singleContext() = new FContext[Json] {
        var value: Json = null
        def add(s: String) = { value = jstring(s) }
        def add(v: Json) = { value = v }
        def finish: Json = value
        def isObj: Boolean = false
      }

      def arrayContext() = new FContext[Json] {
        val vs = mutable.ListBuffer.empty[Json]
        def add(s: String) = { vs += jstring(s); () }
        def add(v: Json) = { vs += v; () }
        def finish: Json = Json.jArray(vs.toList)
        def isObj: Boolean = false
      }

      def objectContext() = new FContext[Json] {
        var key: String = null
        var vs = JsonObject.empty
        def add(s: String): Unit =
          if (key == null) { key = s } else { vs = vs + (key, jstring(s)); key = null }
        def add(v: Json): Unit =
        { vs = vs + (key, v); key = null }
        def finish = Json.jObject(vs)
        def isObj = true
      }
    }

  implicit val lineDelimitedJsonDecoder: EntityDecoder[List[Json]] = {
    EntityDecoder.decodeBy(JsonFormat.LineDelimited.mediaType) { msg =>
      DecodeResult {
        msg.body.parseJson(AsyncParser.ValueStream).partialAttempt {
          case pe: ParseException =>
            Process.emit(ParseFailure("Invalid JSON", pe.getMessage))
        }.runLog.map(_.toList.sequenceU)
      }
    }
  }

  // Remove once version 0.8.4 or higher of jawn is realeased.
  implicit val normalJsonBugFreeDecoder = org.http4s.jawn.jawnDecoder(bugFreeArgonautFacade)

  "Data Service" should {
    "GET" >> {
      "respond with NotFound" >> {
        "if file does not exist" >> {
          val unexistantFile = rootDir[Sandboxed] </> dir("foo") </> file("bar.json")
          val path:String = posixCodec.printPath(unexistantFile)
          val response = service(InMemState.empty)(Request(uri = Uri(path = path))).run
          response.status must_== Status.NotFound
          response.as[String].run must_== "/foo/bar.json: doesn't exist"
        }
      }
      "respond with file data" >> {
        val sampleFile = rootDir[Sandboxed] </> dir("foo") </> file("bar")
        val samplePath: String = posixCodec.printPath(sampleFile)
        def fileSystemWithSampleFile(data: Vector[Data]) = InMemState fromFiles Map(sampleFile -> data)
        "json by default" >> {
          def isExpectedResponse(
            data: Vector[Data], response: Response,
            format: JsonContentType = JsonContentType(Readable, LineDelimited)
          ) = {
            val expectedData = data.map(format.mode.codec.encode).sequenceU.toOption.get
            format.format match {
              case LineDelimited  => response.as[List[Json]].run must_== expectedData.toList
              case SingleArray    => response.as[Json].run must_== Json.array(expectedData: _*)
            }
            response.status must_== Status.Ok
            response.contentType must_== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
          }
          "readable by default" ! prop { data: Vector[Data] =>
            val response = service(fileSystemWithSampleFile(data))(Request(uri = Uri(path = samplePath))).run
            isExpectedResponse(data, response)
          }
          "precise if specified" >> {
            "in the content-type header" ! prop { data: Vector[Data] =>
              val contentType = JsonContentType(Precise,LineDelimited)
              val request = Request(
                uri = Uri(path = samplePath),
                headers = Headers(Accept(contentType.mediaType)))
              val response = service(fileSystemWithSampleFile(data))(request).run
              isExpectedResponse(data, response, contentType)
            }
            "in the request-headers" ! prop { data: Vector[Data] =>
              val request = Request(
                uri = Uri(path = samplePath).+?("request-headers", """{"Accept": "application/ldjson; mode=precise" }"""))
              val response = HeaderParam(service(fileSystemWithSampleFile(data)))(request).run
              isExpectedResponse(data, response, JsonContentType(Precise,LineDelimited))
            }
          }
          "complicated Accept Header" ! prop { data: Vector[Data] =>
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(Header("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise")))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, JsonContentType(Precise,LineDelimited))
          }
          "in array if specified" ! prop { data: Vector[Data] =>
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(Accept(MediaType.`application/json`)))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, JsonContentType(Readable, SingleArray))
          }
          "with gziped encoding when specified" ! prop { data: Vector[Data] =>
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
            val response = GZip(service(fileSystemWithSampleFile(data)))(request).run
            response.headers.get(headers.`Content-Encoding`) must_== Some(`Content-Encoding`(ContentCoding.gzip))
            response.status must_== Status.Ok
          }
          // TODO: Make the filename a property in order to remove this test
          "even if file name contains a space" ! prop { data: Vector[Data] =>
            val fileWithSpaceInName = rootDir[Sandboxed] </> dir("foo") </> file("bar bix.json")
            val path: String = posixCodec.printPath(fileWithSpaceInName)
            val filesystem = InMemState fromFiles Map(fileWithSpaceInName -> data)
            val response = service(filesystem)(Request(uri = Uri(path = path))).run
            isExpectedResponse(data, response)
          }
          "support disposition" ! prop { data: Vector[Data] =>
            val disposition = `Content-Disposition`("attachement", Map("filename" -> "data.json"))
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(Accept(jsonReadableLine.copy(disposition = Some(disposition)).mediaType)))
            val response = service(fileSystemWithSampleFile(data))(request).run
            response.headers.get(`Content-Disposition`) must_== Some(disposition)
          }
          "support offset and limit" >> {
            "return expected result if user supplies valid values" ! prop { (data: Vector[Data], offset: Int, limit: Int) =>
              (offset > 0 && limit >= 1) ==> {
                val request = Request(
                  uri = Uri(path = samplePath).+?("offset", offset.toString).+?("limit", limit.toString))
                val response = service(fileSystemWithSampleFile(data))(request).run
                isExpectedResponse(data.slice(offset, limit), response)
              }
            }
            "return 400 if provided with" >> {
              "a non-positive limit (0 is invalid)" ! prop { (offset: Natural, limit: Int) =>
                (limit < 1) ==> {
                  val request = Request(
                    uri = Uri(path = samplePath).+?("offset", offset.value.toString).+?("limit", limit.toString))
                  val response = service(InMemState.empty)(request).run
                  response.status must_== Status.BadRequest
                  response.as[String].run must_== s"invalid limit: $limit (must be >= 1)"
                }
              }
              "a negative offset" ! prop { (offset: Int, limit: Positive) =>
                (offset < 0) ==> {
                  val request = Request(
                    uri = Uri(path = samplePath).+?("offset", offset.toString).+?("limit", limit.value.toString))
                  val response = service(InMemState.empty)(request).run
                  response.status must_== Status.BadRequest
                  response.as[String].run must_== s"invalid offset: $offset (must be >= 0)"
                }
              }
              "if provided with multiple limits?" ! prop { (offset: Natural, limits: List[Positive]) =>
                (limits.length >= 2) ==> {
                  val request = Request(
                    uri = Uri(path = samplePath).+?("offset", offset.value.toString).+?("limit", limits.map(_.value.toString)))
                  val response = service(InMemState.empty)(request).run
                  response.status must_== Status.BadRequest
                  response.as[String].run must_== s"Two limits were provided, only supply one limit"
                }
              }
              "if provided with multiple offsets?" ! prop { (limit: Positive, offsets: List[Natural]) =>
                (offsets.length >= 2) ==> {
                  val request = Request(
                    uri = Uri(path = samplePath).+?("offset", offsets.map(_.value.toString)).+?("limit", limit.value.toString))
                  val response = service(InMemState.empty)(request).run
                  response.status must_== Status.BadRequest
                  response.as[String].run must_== s"Two offsets were provided, only supply one offset"
                  todo // Confirm this is the expected behavior because http4s defaults to just grabbing the first one
                       // and going against that default behavior would be more work
                }
              }
              "an unparsable limit" >> {
                val request = Request(
                  uri = Uri(path = samplePath).+?("limit", "a"))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"""invalid limit: Query decoding Long failed (For input string: "a")"""
              }
              "if provided with both an invalid offset and limit" ! prop { (limit: Int, offset: Int) =>
                (limit < 1 && offset < 0) ==> {
                  val request = Request(
                    uri = Uri(path = samplePath).+?("limit", limit.toString).+?("offset", offset.toString))
                  val response = service(InMemState.empty)(request).run
                  response.status must_== Status.BadRequest
                  response.as[String].run must_== s"invalid limit: $limit (must be >= 1), invalid offset: $offset (must be >= 0)"
                }
              }
            }
          }
          "support very large data set" >> {
            val data = (0 until 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))).toVector
            "plain text" >> {
              val request = Request(
                uri = Uri(path = samplePath))
              val response = service(fileSystemWithSampleFile(data))(request).run
              isExpectedResponse(data, response)
            }
            "gziped" >> {
              val request = Request(
                uri = Uri(path = samplePath),
                headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
              val response = service(fileSystemWithSampleFile(data))(request).run
              isExpectedResponse(data, response)
            }
          }
        }
        "csv if specified" >> {
          val simpleData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.Int(2))),
            Data.Obj(ListMap("c" -> Data.Set(List(Data.Int(3))))))
          val simpleExpected = List("a,b,c[0]", "1,,", ",2,", ",,3")
          def test(
                  data: List[Data],
                  expectedLines: List[String],
                  requestMediaType: MediaType = MediaType.`text/csv`,
                  expectedResponseMediaType: MediaType = MessageFormat.Csv.Default.mediaType) = {
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(headers.Accept(requestMediaType))
            )
            val response = service(fileSystemWithSampleFile(data.toVector))(request).run
            response.status must_== Status.Ok
            response.contentType must_== Some(expectedResponseMediaType)
            response.as[String].run must_== expectedLines.mkString("\n")
          }

          "simple" >> test(
            data = simpleData,
            expectedLines = simpleExpected
          )
          "with quoting" >> test(
            data = List(
              Data.Obj(ListMap(
                "a" -> Data.Str("\"Hey\""),
                "b" -> Data.Str("a, b, c")))),
            expectedLines = List("a,b", "\"\"\"Hey\"\"\",\"a, b, c\"")
          )
          "specifying format exactly" >> {
            "alternative delimiters" >> {
              val extensions = Map(
                "columnDelimiter" -> "\"\t\"",
                "rowDelimiter" -> "\";\"",
                "quoteChar" -> "\"'\"",
                "escapeChar" -> "\"\\\\\""
              )
              val alternative = MediaType.`text/csv`.withExtensions(extensions)
              test(
                data = simpleData,
                expectedLines = List("a\tb\tc[0];1\t\t;\t2\t;\t\t3;"),
                requestMediaType = alternative,
                expectedResponseMediaType = alternative
              )
            }
            "the default parameters" >> test(
              data = simpleData,
              expectedLines = simpleExpected,
              requestMediaType = MessageFormat.Csv.Default.mediaType,
              expectedResponseMediaType = MessageFormat.Csv.Default.mediaType
            )
          }
          "using disposition to download as zipped directory" >> {
            val disposition = `Content-Disposition`("attachement", Map("filename" -> "foo.zip"))
            val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(Accept(requestMediaType)))
            val response = service(fileSystemWithSampleFile(simpleData.toVector))(request).run
            response.status must_== Status.Ok
            response.contentType must_== Some(MediaType.`application/zip`)
            response.headers.get(`Content-Disposition`) must_== disposition
          }
        }
        "what happens if user specifies a Path that is a directory but without the appropriate headers?" >> todo
      }
    }
    "POST and PUT" >> {
      def testBoth[A](test: org.http4s.Method => Unit) = {
        "POST" should {
          test(org.http4s.Method.POST)
        }
        "PUT" should {
          test(org.http4s.Method.PUT)
        }
      }
      testBoth { method =>
        "be 415 if media-type is missing" ! prop { file: Path[Abs,File,Sandboxed] =>
          val path = posixCodec.printPath(file)
          val request = Request(
            uri = Uri(path = path),
            method = method).withBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}").run
          val response = service(emptyMem)(request).run
          response.status must_== Status.UnsupportedMediaType
          response.as[String].run must_== "No media-type is specified in Content-Type header"
        }
        "be 400 with" >> {
          def be400(body: String, expectedBody: String, mediaType: MediaType = jsonReadableLine.mediaType) = {
            prop { file: Path[Abs, File, Sandboxed] =>
              val path = posixCodec.printPath(file)
              val request = Request(
                uri = Uri(path = path),
                headers = Headers(`Content-Type`(mediaType, Charset.`UTF-8`))).withBody(body).run
              val response = service(emptyMem)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== expectedBody
              // TODO: Assert nothing changed in the backend
            }
          }
          "invalid body" >> {
            "no body" ! be400(body = "", expectedBody = "Request has no body")
            "invalid JSON" ! be400(body = "{", expectedBody = "Some uploaded value(s) could not be processed")
            "invalid CSV" >> {
              "empty (no headers)" ! {
                be400(body = "", expectedBody = "Some uploaded value(s) could not be processed", csv)
              }
              "if broken (after the tenth data line)" ! {
                val brokenBody = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n"
                be400(body = brokenBody, expectedBody = "Some uploaded value(s) could not be processed", csv)
              }
            }
          }
          // TODO: Consider spliting this into a case of Root (depth == 0) and missing dir (depth > 1)
          "if path is invalid (parent directory does not exist)" ! prop { (file: Path[Abs,File,Sandboxed], json: Json) =>
            Path.depth(file) != 1 ==> {
              be400(body = json.spaces4, s"Invalid path: ${posixCodec.printPath(file)}")
            }
          }
        }
        "accept valid data" >> {
          def accept(body: String, expected: List[Data], mediaType: MediaType) =
            prop { file: Path[Abs, File, Sandboxed] =>
              (Path.depth(file) == 1) ==> {
                val path = posixCodec.printPath(file)
                val request = Request(
                  uri = Uri(path = path),
                  method = method,
                  headers = Headers(`Content-Type`(mediaType, Charset.`UTF-8`))).withBody(body).run
                val response = service(emptyMem)(request).run
                response.status must_== Status.Ok
                response.as[String] must_== ""
                // TODO: How do I check the in-memory interpreter to find out if the files were create
                // also need to test for Append vs Save
              }
            }
          val expectedData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))
          "Json" >> {
            def formatAsMultiLineArray(jsonBlob: String) = {
              // insert a comma at the end of each line (because we are in an array)
              val adaptedBlob = jsonBlob.split("\n").mkString(",\n")
              s"[$adaptedBlob]"
            }
            def formatAsSingleLineArray(jsonBlob: String) = {
              // Remove the newline and replace with a comma only (because we are in an array)
              val adaptedBlob = jsonBlob.split("\n").mkString(",")
              s"[$adaptedBlob]"
            }
            def testPreciseAndReadable[A](test: (JsonPrecision, String) => Unit) = {
              "Precise" should {
                val jsonBlob =
                  """{"a" : 1}
                    |{"b" : {"$time": "12:34:56"}}""".stripMargin
                test(Precise, jsonBlob)
              }//.pendingUntilFixed("SD-1066") Damn you specs2 2.x!!
              "Readable" should {
                val jsonBlob =
                  """{"a" : 1}
                    |{"b" : "12:34:56"}""".stripMargin
                test(Readable, jsonBlob)
              }
            }
            testPreciseAndReadable { (precision, jsonBlob) =>
              "when formatted with one json object per line" ! {
                accept(jsonBlob, expectedData, JsonContentType(precision, LineDelimited).mediaType)
              }
              "when formatted as a single json array" ! {
                accept(formatAsMultiLineArray(jsonBlob), List(Data.Arr(expectedData)), JsonContentType(precision, SingleArray).mediaType)
              }
              "when having multiple lines containing arrays" ! {
                val arbitraryValue = 3
                def replicate[A](a: A) = Applicative[Id].replicateM[A](arbitraryValue, a)
                val jsonString = replicate(formatAsSingleLineArray(jsonBlob)).mkString("\n")
                accept(jsonString, replicate(Data.Arr(expectedData)), JsonContentType(precision, LineDelimited).mediaType)
              }
              () // Eliminates warning, specs2 3.x will allow us to remove
            }
          }
          "CSV" >> {
            "standard" >> {
              accept("a,b\n1,\n,12:34:56", expectedData, MediaType.`text/csv`)
            }
            "weird" >> {
              val weirdData = List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))
              accept("a|b\n1|\n|'[1|2|3]'\n", weirdData, MediaType.`text/csv`)
            }
          }
        }
        "produce two errors with partially invalid JSON" ! prop { path: Path[Abs,File,Sandboxed] =>
          val twoErrorJson = """{"a": 1}
                               |"unmatched
                               |{"b": 2}
                               |}
                               |{"c": 3}""".stripMargin
          val pathString = posixCodec.printPath(path)
          val request = Request(
            uri = Uri(path = pathString),
            method = method,
            headers = Headers(`Content-Type`(jsonReadableLine.mediaType))).withBody(twoErrorJson).run
          val response = service(emptyMem)(request).run
          val jsonResponse = response.as[Json].run
          jsonResponse must_== Json("details" := "expected two error messages") // TODO: Put actual error messages
          // TODO: Make sure the filesystem has not been changed?
        }
        () // Required for testBoth (not required with specs2 3.x)
      }
    }
    "PUT" >> {
      "be 500 with simualated error on a particular value" >> {
        todo
        // TODO: Understand what this test is meant to assert
      }
    }
    "POST" >> {
      "be 500 with simulated error on a particular value" >> {
        todo
        // TODO: Understand what this test is meant to assert
      }
    }
    "MOVE" >> {
      "be 400 for missing Destination header" ! prop { path: Path[Abs,File,Sandboxed] =>
        val pathString = posixCodec.printPath(path)
        val request = Request(uri = Uri(path = pathString), method = Method.MOVE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.BadRequest
        response.as[String].run must_== "The 'Destination' header must be specified"
      }
      "be 404 for missing source file" ! prop { (source: Path[Abs,File,Sandboxed], destination: Path[Abs,File,Sandboxed]) =>
        val sourceString = posixCodec.printPath(source)
        val destinationString = posixCodec.printPath(destination)
        val request = Request(uri = Uri(path = sourceString), headers = Headers(Header("Destination", destinationString)), method = Method.MOVE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.NotFound
        response.as[String].run must_== s"$sourceString: doesn't exist"
      }
      "be 404 for missing destination file" ! prop { (source: Path[Abs,File,Sandboxed], destination: Path[Abs,File,Sandboxed]) =>
        todo // Figure out what we are doing with nested backends because not sure this test makes sense without them
      }
      "be 201 with file" ! prop {(source: Path[Abs,File,Sandboxed], destination: Path[Abs,File,Sandboxed], data: Vector[Data]) =>
        val sourceString = posixCodec.printPath(source)
        val destinationString = posixCodec.printPath(destination)
        val request = Request(uri = Uri(path = sourceString), headers = Headers(Header("Destination", destinationString)), method = Method.MOVE)
        val filesystem = InMemState.fromFiles(Map(source -> data))
        val response = service(filesystem)(request).run
        response.status must_== Status.Created
        // TODO: Check that the file was moved properly (not in old test though...)
      }
      "be 201 with dir" ! prop {(sourceDir: Path[Abs,Dir,Sandboxed], sampleFiles: Map[String, Vector[Data]], destinationDir: Path[Abs,Dir,Sandboxed]) =>
        val sourceDirString = posixCodec.printPath(sourceDir)
        val fileMapping = sampleFiles.map{ case (fileName,data) => (sourceDir </> file(fileName), data)}
        val destinationString = posixCodec.printPath(destinationDir)
        val request = Request(uri = Uri(path = sourceDirString), headers = Headers(Header("Destination", destinationString)), method = Method.MOVE)
        val filesystem = InMemState fromFiles fileMapping
        val response = service(filesystem)(request).run
        response.status must_== Status.Created
        // TODO: Check that the file was moved properly (not in old test though...)
      }
      "be 501 for src and dst not in the same backend" >> todo // What are we doing? make sure that we test all error conditions that the backend can return
    }
    "DELETE" >> {
      "be 200 with existing file" ! prop { (file: Path[Abs,File,Sandboxed], data: Vector[Data]) =>
        val path = posixCodec.printPath(file)
        val request = Request(uri = Uri(path = path), method = Method.DELETE)
        val filesystem = InMemState fromFiles (Map(file -> data))
        val response = service(filesystem)(request).run
        response.status must_== Status.Ok
        // Check if file was actually deleted
      }
      "be 200 with existing dir" ! prop { (dir: Path[Abs,Dir,Sandboxed], sampleFiles: Map[String, Vector[Data]]) =>
        val dirPath = posixCodec.printPath(dir)
        val fileMapping = sampleFiles.map{ case (fileName,data) => (dir </> file(fileName), data)}
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val filesystem = InMemState fromFiles fileMapping
        val response = service(filesystem)(request).run
        response.status must_== Status.Ok
        // Check if dir was actually deleted
      }
      "be 200 with missing file (idempotency)" ! prop { (file: Path[Abs,File,Sandboxed], data: Vector[Data]) =>
        val path = posixCodec.printPath(file)
        val request = Request(uri = Uri(path = path), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.Ok
        // Check if still empty?
      }
      "be 200 with missing dir (idempotency)" ! prop { dir: Path[Abs,Dir,Sandboxed] =>
        val dirPath = posixCodec.printPath(dir)
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.Ok
        // Check if dir was actually deleted
      }
    }
  }
}
