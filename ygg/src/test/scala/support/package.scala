package ygg

import common._
import ygg.macros.Json

package object tests extends ygg.tests.pkg.TestsPackage {
  type CogroupData = SampleData -> SampleData

  implicit def jsonStringContext(sc: StringContext): Json.JsonStringContext = new Json.JsonStringContext(sc)
}
