package quasar
package api
package services

import jawn.{FContext, Facade, AsyncParser}
import org.http4s.server.middleware.GZip
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import quasar.Data
import quasar.Predef._
import quasar.api.MessageFormat.JsonContentType
import JsonPrecision._
import JsonFormat._
import quasar.DataCodec

import argonaut.{Argonaut, JsonObject, JsonNumber, Json}
import argonaut.Argonaut._
import org.http4s._
import org.http4s.headers._
import org.http4s.server._
import quasar.fs.{Path => _, _}
import pathy.Path
import pathy.Path._
import quasar.fs.PathyGen._
import quasar.fs.NumericGen._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

import quasar.api.MessageFormatGen._

import org.scalacheck.Arbitrary

import Fixture._

class DataServiceSpec extends Specification with ScalaCheck with FileSystemFixture with Http4s {
  import inmemory._

  def service(mem: InMemState): HttpService =
    data.service[FileSystem](runStatefully(mem).run.compose(fileSystem))

  implicit val arbJson: Arbitrary[Json] = Arbitrary(Arbitrary.arbitrary[String].map(jString(_)))

  val csv = MediaType.`text/csv`

  import posixCodec.printPath

  "Data Service" should {
    "GET" >> {
      "respond with NotFound" >> {
        "if file does not exist" ! prop { file: AbsFile[Sandboxed] =>
          val response = service(InMemState.empty)(Request(uri = Uri(path = printPath(file)))).run
          response.status must_== Status.NotFound
          response.as[String].run must_== s"${printPath(file)}: doesn't exist"
        }
      }
      "respond with file data" >> {
        def isExpectedResponse(data: Vector[Data], response: Response, format: MessageFormat) = {
          val expectedBody: Process[Task, String] = format.encode(Process.emitAll(data))
          response.as[String].run must_== expectedBody.runLog.run.mkString("")
          response.status must_== Status.Ok
          response.contentType must_== Some(`Content-Type`(format.mediaType, Charset.`UTF-8`))
        }
        "in correct format" >> {
          "readable and line delimited json by default" ! prop { filesystem: SingleFileFileSystem =>
            val response = service(filesystem.state)(Request(uri = Uri(path = filesystem.path))).run
            isExpectedResponse(filesystem.contents, response, MessageFormat.Default)
          }
          "in any supported format if specified" >> {
            "in the content-type header" >> {
              def testProp(format: MessageFormat) = prop { filesystem: SingleFileFileSystem => test(format, filesystem) }
              def test(format: MessageFormat, filesystem: SingleFileFileSystem) = {
                val request = Request(
                  uri = Uri(path = filesystem.path),
                  headers = Headers(Accept(format.mediaType)))
                val response = service(filesystem.state)(request).run
                isExpectedResponse(filesystem.contents, response, format)
              }
              "json" >> {
                s"readable and line delimited (${jsonReadableLine.mediaType.renderString})" >> {
                  testProp(jsonReadableLine)
                }
                s"precise and line delimited (${jsonPreciseLine.mediaType.renderString})" >> {
                  testProp(jsonPreciseLine)
                }
                s"readable and in a single json array (${jsonReadableArray.mediaType.renderString})" >> {
                  testProp(jsonReadableArray)
                }
                s"precise and in a single json array (${jsonPreciseArray.mediaType.renderString})" >> {
                  testProp(jsonPreciseArray)
                }
              }
              "csv" ! prop { (filesystem: SingleFileFileSystem, format: MessageFormat.Csv) => test(format, filesystem) }
              "or a more complicated proposition" ! prop { filesystem: SingleFileFileSystem =>
                val request = Request(
                  uri = Uri(path = filesystem.path),
                  headers = Headers(Header("Accept", "application/ldjson;q=0.9;mode=readable,application/json;boundary=NL;mode=precise")))
                val response = service(filesystem.state)(request).run
                isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
              }
            }
            "in the request-headers" ! prop { filesystem: SingleFileFileSystem =>
              val contentType = JsonContentType(Precise, LineDelimited)
              val request = Request(
                uri = Uri(path = filesystem.path).+?("request-headers", s"""{"Accept": "application/ldjson; mode=precise" }"""))
              val response = HeaderParam(service(filesystem.state))(request).run
              isExpectedResponse(filesystem.contents, response, JsonContentType(Precise, LineDelimited))
            }
          }
        }
        "with gziped encoding when specified" ! prop { filesystem: SingleFileFileSystem =>
          val request = Request(
            uri = Uri(path = filesystem.path),
            headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
          val response = GZip(service(filesystem.state))(request).run
          response.headers.get(headers.`Content-Encoding`) must_== Some(`Content-Encoding`(ContentCoding.gzip))
          response.status must_== Status.Ok
        }
        "support disposition" ! prop { filesystem: SingleFileFileSystem =>
          val disposition = `Content-Disposition`("attachement", Map("filename" -> "data.json"))
          val request = Request(
            uri = Uri(path = filesystem.path),
            headers = Headers(Accept(jsonReadableLine.copy(disposition = Some(disposition)).mediaType)))
          val response = service(filesystem.state)(request).run
          response.headers.get(`Content-Disposition`) must_== Some(disposition)
        }
        "support offset and limit" >> {
          "return expected result if user supplies valid values" ! prop {
            (filesystem: SingleFileFileSystem, offset: Natural, limit: Positive, format: MessageFormat) =>
            // Not sure why this precondition is necessary...
            (offset.value < Int.MaxValue && limit.value < Int.MaxValue) ==> {
              val request = Request(
                uri = Uri(path = filesystem.path).+?("offset", offset.value.toString).+?("limit", limit.value.toString),
                headers = Headers(Accept(format.mediaType)))
              val response = service(filesystem.state)(request).run
              isExpectedResponse(filesystem.contents.drop(offset.value.toInt).take(limit.value.toInt), response, format)
            }
          }
          "return 400 if provided with" >> {
            "a non-positive limit (0 is invalid)" ! prop { (path: AbsFile[Sandboxed], offset: Natural, limit: Int) =>
              (limit < 1) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("offset", offset.value.toString).+?("limit", limit.toString))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"invalid limit: $limit (must be >= 1)"
              }
            }
            "a negative offset" ! prop { (path: AbsFile[Sandboxed], offset: Negative, limit: Positive) =>
              val request = Request(
                uri = Uri(path = printPath(path)).+?("offset", offset.value.toString).+?("limit", limit.value.toString))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"invalid offset: ${offset.value} (must be >= 0)"
            }
            "if provided with multiple limits?" ! prop { (path: AbsFile[Sandboxed], offset: Natural, limit1: Positive, limit2: Positive, otherLimits: List[Positive]) =>
              val limits = limit1 :: limit2 :: otherLimits
              val request = Request(
                uri = Uri(path = printPath(path)).+?("offset", offset.value.toString).+?("limit", limits.map(_.value.toString)))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"Two limits were provided, only supply one limit"
            }.pendingUntilFixed("SD-1082")
            "if provided with multiple offsets?" ! prop { (path: AbsFile[Sandboxed], limit: Positive, offsets: List[Natural]) =>
              (offsets.length >= 2) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("offset", offsets.map(_.value.toString)).+?("limit", limit.value.toString))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"Two offsets were provided, only supply one offset"
                todo // Confirm this is the expected behavior because http4s defaults to just grabbing the first one
                     // and going against that default behavior would be more work
              }
            }.pendingUntilFixed("SD-1082")
            "an unparsable limit" ! prop { path: AbsFile[Sandboxed] =>
              val request = Request(
                uri = Uri(path = printPath(path)).+?("limit", "a"))
              val response = service(InMemState.empty)(request).run
              response.status must_== Status.BadRequest
              response.as[String].run must_== s"""invalid limit: Query decoding Long failed (For input string: "a")"""
            }
            "if provided with both an invalid offset and limit" ! prop { (path: AbsFile[Sandboxed], limit: Int, offset: Negative) =>
              (limit < 1) ==> {
                val request = Request(
                  uri = Uri(path = printPath(path)).+?("limit", limit.toString).+?("offset", offset.value.toString))
                val response = service(InMemState.empty)(request).run
                response.status must_== Status.BadRequest
                response.as[String].run must_== s"invalid limit: $limit (must be >= 1), invalid offset: ${offset.value} (must be >= 0)"
              }
            }.pendingUntilFixed("SD-1083")
          }
        }
        "support very large data set" >> {
          val sampleFile = rootDir[Sandboxed] </> dir("foo") </> file("bar")
          val samplePath: String = printPath(sampleFile)
          def fileSystemWithSampleFile(data: Vector[Data]) = InMemState fromFiles Map(sampleFile -> data)
          val data = (0 until 100*1000).map(n => Data.Obj(ListMap("n" -> Data.Int(n)))).toVector
          "plain text" >> {
            val request = Request(
              uri = Uri(path = samplePath))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, MessageFormat.Default)
          }
          "gziped" >> {
            val request = Request(
              uri = Uri(path = samplePath),
              headers = Headers(`Accept-Encoding`(org.http4s.ContentCoding.gzip)))
            val response = service(fileSystemWithSampleFile(data))(request).run
            isExpectedResponse(data, response, MessageFormat.Default)
          }
        }
      }
      "using disposition to download as zipped directory" ! prop { filesystem: NonEmptyDir =>
        val dirPath = printPath(filesystem.dir)
        val disposition = `Content-Disposition`("attachement", Map("filename" -> "foo.zip"))
        val requestMediaType = MediaType.`text/csv`.withExtensions(Map("disposition" -> disposition.value))
        val request = Request(
          uri = Uri(path = dirPath),
          headers = Headers(Accept(requestMediaType)))
        val response = service(filesystem.state)(request).run
        response.status must_== Status.Ok
        response.contentType must_== Some(`Content-Type`(MediaType.`application/zip`))
        response.headers.get(`Content-Disposition`) must_== disposition
      }.pendingUntilFixed("Seem to have some problems testing directories, also, I don't think is specific to csv") // TODO: FIXME
      "what happens if user specifies a Path that is a directory but without the appropriate headers?" >> todo
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
          val path = printPath(file)
          val request = Request(
            uri = Uri(path = path),
            method = method).withBody("{\"a\": 1}\n{\"b\": \"12:34:56\"}").run
          val response = service(emptyMem)(request).run
          response.status must_== Status.UnsupportedMediaType
          response.as[String].run must_== "No media-type is specified in Content-Type header"
        }
        "be 400 with" >> {
          def be400[A: EntityDecoder](body: String, expectedBody: A, mediaType: MediaType = jsonReadableLine.mediaType) = {
            prop { file: Path[Abs, File, Sandboxed] =>
              val path = printPath(file)
              val request = Request(
                uri = Uri(path = path),             // We do it this way becuase withBody sets the content-type
                method = method).withBody(body).run.replaceAllHeaders(`Content-Type`(mediaType, Charset.`UTF-8`))
              val response = service(emptyMem)(request).run
              response.as[A].run must_== expectedBody
              response.status must_== Status.BadRequest
              // TODO: Assert nothing changed in the backend
            }
          }
          "invalid body" >> {
            "no body" ! be400(body = "", expectedBody = "Request has no body")
            "invalid JSON" ! be400(
              body = "{",
              expectedBody = Json("error" := "some uploaded value(s) could not be processed",
                                  "details" := Json.array(jString("parse error: JSON terminates unexpectedly. in the following line: {")))
            )
            "invalid CSV" >> {
              "empty (no headers)" ! be400(
                body = "",
                expectedBody = "Request has no body",
                mediaType = csv
              )
              "if broken (after the tenth data line)" ! {
                val brokenBody = "\"a\",\"b\"\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n\",\n"
                be400(
                  body = brokenBody,
                  expectedBody = Json("error" := "some uploaded value(s) could not be processed",
                    "details" := Json.array(jString("parse error: Malformed Input!: Some(\",\n)"))),
                  mediaType = csv)
              }
            }
          }
          // TODO: Consider spliting this into a case of Root (depth == 0) and missing dir (depth > 1)
          "if path is invalid (parent directory does not exist)" ! prop { (file: Path[Abs,File,Sandboxed], json: Json) =>
            Path.depth(file) != 1 ==> {
              be400(body = json.spaces4, s"Invalid path: ${printPath(file)}")
            }
          }.pendingUntilFixed("What do we want here, create it or not?")
        }
        "accept valid data" >> {
          def accept[A: EntityEncoder](body: A, expected: List[Data]) =
            prop { fileName: NonEmptyString =>
              val sampleFile = rootDir[Sandboxed] </> file(fileName.value)
              val path = printPath(sampleFile)
              val request = Request(uri = Uri(path = path), method = method).withBody(body).run
              val response = service(emptyMem)(request).run
              response.as[String].run must_== ""
              response.status must_== Status.Ok
              // TODO: How do I check the in-memory interpreter to find out if the files were create
              // also need to test for Append vs Save
            }
          val expectedData = List(
            Data.Obj(ListMap("a" -> Data.Int(1))),
            Data.Obj(ListMap("b" -> Data.Time(org.threeten.bp.LocalTime.parse("12:34:56")))))
          "Json" >> {
            val line1 = Json("a" := 1)
            val preciseLine2 = Json("b" := Json("$time" := "12:34:56"))
            val readableLine2 = Json("b" := "12:34:56")
            "when formatted with one json object per line" >> {
              "Precise" ! {
                accept(List(line1, preciseLine2).map(PreciseJson(_)), expectedData)
              }
              "Readable" ! {
                accept(List(line1, readableLine2).map(ReadableJson(_)), expectedData)
              }
            }
            "when formatted as a single json array" >> {
              "Precise" ! {
                accept(PreciseJson(Json.array(line1, preciseLine2)), List(Data.Arr(expectedData)))
              }
              "Readable" ! {
                accept(ReadableJson(Json.array(line1, readableLine2)), List(Data.Arr(expectedData)))
              }
            }
            "when having multiple lines containing arrays" >> {
              val arbitraryValue = 3
              def replicate[A](a: A) = Applicative[Id].replicateM[A](arbitraryValue, a)
              "Precise" ! {
                accept(PreciseJson(Json.array(replicate(Json.array(line1, preciseLine2)): _*)), replicate(Data.Arr(expectedData)))
              }
              "Readable" ! {
                accept(ReadableJson(Json.array(replicate(Json.array(line1, readableLine2)): _*)), replicate(Data.Arr(expectedData)))
              }
            }
          }
          "CSV" >> {
            "standard" >> {
              accept(Csv("a,b\n1,\n,12:34:56"), expectedData)
            }
            "weird" >> {
              val weirdData = List(
                Data.Obj(ListMap("a" -> Data.Int(1))),
                Data.Obj(ListMap("b" -> Data.Str("[1|2|3]"))))
              accept(Csv("a|b\n1|\n|'[1|2|3]'\n"), weirdData)
            }
          }
        }
        "produce two errors with partially invalid JSON" ! prop { path: Path[Abs,File,Sandboxed] =>
          val twoErrorJson = """{"a": 1}
                               |"unmatched
                               |{"b": 2}
                               |}
                               |{"c": 3}""".stripMargin
          val pathString = printPath(path)
          val request = Request(
            uri = Uri(path = pathString),               // withBody replaces the headers which is why we need to specify them here
            method = method).withBody(twoErrorJson).run.replaceAllHeaders(Headers(`Content-Type`(jsonReadableLine.mediaType)))
          val response = service(emptyMem)(request).run
          response.status must_== Status.BadRequest // TODO: Is this really what we want?
          val jsonResponse = response.as[Json].run
          jsonResponse must_== Json("error" := "some uploaded value(s) could not be processed",
            "details" := Json.array(
              jString("parse error: JSON terminates unexpectedly. in the following line: \"unmatched"),
              jString("parse error: Unexpected content found: } in the following line: }")
            ))
          // TODO: Make sure the filesystem has not been changed?
        }
        "be 500 with server side error" >> todo
        () // Required for testBoth (not required with specs2 3.x)
      }
    }
    "MOVE" >> {
      def testMove[A: EntityDecoder](from: AbsPath[Sandboxed],
                                     to: AbsPath[Sandboxed], state: InMemState, expectedBody: A, status: Status) = {
        // TODO: Consider it's possible to invent syntax Move(...)
        val request = Request(
          uri = Uri(path = from.fold(printPath, printPath)),
          headers = Headers(Header("Destination", to.fold(printPath, printPath))),
          method = Method.MOVE)
        val response = service(state)(request).run
        response.status must_== status
        response.as[A].run must_== expectedBody
      }
      "be 400 for missing Destination header" ! prop { path: AbsFile[Sandboxed] =>
        val pathString = printPath(path)
        val request = Request(uri = Uri(path = pathString), method = Method.MOVE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.BadRequest
        response.as[String].run must_== "The 'Destination' header must be specified"
      }
      "be 404 for missing source file" ! prop { (file: AbsFile[Sandboxed], destFile: AbsFile[Sandboxed]) =>
        testMove(
          from = file.right,
          to = destFile.right,
          state = emptyMem,
          status = Status.NotFound,
          expectedBody = s"${printPath(file)}: doesn't exist")
      }
      "be 404 if attempting to move a dir into a file" ! prop {(fs: NonEmptyDir, file: AbsFile[Sandboxed]) =>
        testMove(
          from = fs.dir.left,
          to = file.right,
          state = fs.state,
          status = Status.BadRequest,
          expectedBody = "Cannot move directory into a file")
      }
      "be 404 if attempting to move a file into a dir" ! prop {(fs: SingleFileFileSystem, dir: AbsDir[Sandboxed]) =>
        testMove(
          from = fs.file.right,
          to = dir.left,
          state = fs.state,
          status = Status.BadRequest,
          expectedBody = "Cannot move a file into a directory, must specify destination precisely")
      }
      "be 201 with file" ! prop {(fs: SingleFileFileSystem, file: AbsFile[Sandboxed]) =>
        testMove(
          from = fs.file.right,
          to = file.right,
          state = fs.state,
          status = Status.Created,
          expectedBody = "")
        // TODO: Check that the file was moved properly (not in old test though...)
      }
      "be 201 with dir" ! prop {(fs: NonEmptyDir, dir: AbsDir[Sandboxed]) =>
        testMove(
          from = fs.dir.left,
          to = dir.left,
          state = fs.state,
          status = Status.Created,
          expectedBody = "")
        // TODO: Check that the file was moved properly (not in old test though...)
      }
    }
    "DELETE" >> {
      "be 200 with existing file" ! prop { filesystem: SingleFileFileSystem =>
        val request = Request(uri = Uri(path = filesystem.path), method = Method.DELETE)
        val response = service(filesystem.state)(request).run
        response.status must_== Status.Ok
        // Check if file was actually deleted
      }
      "be 200 with existing dir" ! prop { filesystem: NonEmptyDir =>
        val dirPath = printPath(filesystem.dir)
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val response = service(filesystem.state)(request).run
        response.status must_== Status.Ok
        // Check if dir was actually deleted
      }
      "be 404 with missing file" ! prop { file: AbsFile[Sandboxed] =>
        val path = printPath(file)
        val request = Request(uri = Uri(path = path), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.NotFound
        // Check if still empty?
      }
      "be 404 with missing dir" ! prop { dir: AbsDir[Sandboxed] =>
        val dirPath = printPath(dir)
        val request = Request(uri = Uri(path = dirPath), method = Method.DELETE)
        val response = service(emptyMem)(request).run
        response.status must_== Status.NotFound
        // Check if still empty?
      }
    }
  }
}
