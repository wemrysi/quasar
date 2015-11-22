package quasar
package fs

import quasar.Predef._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

class QueryFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import InMemory._, FileSystemError._, PathError2._, DataGen._

  "QueryFile" should {
    "descendantFiles" >> {
      "returns all descendants of the given directory" ! prop {
        (dp: ADir, dc1: RDir, dc2: RDir, od: ADir, fns: List[String]) =>
          ((dp != od) && depth(dp) > 0 && depth(od) > 0 && fns.nonEmpty) ==> {
            val body = Vector(Data.Str("foo"))
            val fs  = fns take 5 map file
            val f1s = fs map (f => (dp </> dc1 </> f, body))
            val f2s = fs map (f => (dp </> dc2 </> f, body))
            val fds = fs map (f => (od </> f, body))

            val mem = InMemState fromFiles (f1s ::: f2s ::: fds).toMap
            val expectedFiles = (fs.map(dc1 </> _) ::: fs.map(dc2 </> _)).distinct

            InMem.interpret(query.descendantFiles(dp)).eval(mem).toEither must
              beRight(containTheSameElementsAs(expectedFiles))
        }
      }

      "returns not found when dir does not exist" ! prop { d: ADir =>
        InMem.interpret(query.descendantFiles(d)).eval(emptyMem).toEither must beLeft(PathError(PathNotFound(d)))
      }
    }

    "fileExists" >> {
      "return true when file exists" ! prop { s: SingleFileMemState =>
        InMem.interpret(query.fileExists(s.file)).eval(s.state) must beTrue
      }

      "return false when file doesn't exist" ! prop { (absentFile: AFile, s: SingleFileMemState) =>
        InMem.interpret(query.fileExists(absentFile)).eval(s.state) must beFalse
      }

      "return false when dir exists with same name as file" ! prop { (f: AFile, data: Vector[Data]) =>
        val n = fileName(f)
        val fd = parentDir(f).get </> dir(n.value) </> file("different.txt")

        InMem.interpret(query.fileExists(f)).eval(InMemState fromFiles Map(fd -> data)) must beFalse
      }
    }
  }
}
