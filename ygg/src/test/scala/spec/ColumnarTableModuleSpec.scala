package ygg.tests

import SampleData._

class ColumnarTableModuleSpec
      extends ColumnarTableQspec
         with SchemasSpec {

  "a table dataset" should {
    "verify bijection from static JSON" in {
      implicit val gen = sample(schema)
      prop((sd: SampleData) => toJsonSeq(fromJson(sd.data)) must_=== sd.data)
    }

    "in concat" >> {
      "concat two tables" in testConcat
    }

    "in schemas" >> {
      "find a schema in single-schema table" in testSingleSchema
      "find a schema in homogeneous array table" in testHomogeneousArraySchema
      "find schemas separated by slice boundary" in testCrossSliceSchema
      "extract intervleaved schemas" in testIntervleavedSchema
      "don't include undefineds in schema" in testUndefinedsInSchema
      "deal with most expected types" in testAllTypesInSchema
    }
  }
}
