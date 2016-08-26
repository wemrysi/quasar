package ygg.tests

class ToArraySpec extends ColumnarTableQspec {
  "in toArray" >> {
    "create a single column given two single columns" in testToArrayHomogeneous
    "create a single column given heterogeneous data" in testToArrayHeterogeneous
  }

  private def testToArrayHomogeneous = checkTableFun(
    fun = _.toArray[Double],
    data = jsonMany"""
      { "value": 23.4, "key": [ 1 ] }
      { "value": 12.4, "key": [ 2 ] }
      { "value": -12.4, "key": [ 3 ] }
    """,
    expected = jsonMany"[ 23.4 ] [ 12.4 ] [ -12.4 ]"
  )
  private def testToArrayHeterogeneous = checkTableFun(
    fun = _.toArray[Double],
    data = jsonMany"""
      {"key":[1],"value":{"bar":18.8,"foo":23.4}}
      {"key":[2],"value":{"bar":"a","foo":23.4}}
      {"key":[3],"value":{"bar":44.4}}
    """,
    expected = jsonMany"[ 18.8, 23.4 ]"
  )
}
