package ygg.tests

import ygg.json._

object MergeDiffData {
  /** Diff **/
  def expectedChanges: JValue = json"""{
    "tags": ["static-typing","fp"],
    "features": { "key2": "newval2" }
  }"""

  def expectedAdditions: JValue = json"""{
    "features": { "key3": "val3"},
    "compiled": true
  }"""

  def expectedDeletions: JValue = json"""{
    "year":2006,
    "features":{ "key1":"val1" }
  }"""

  /** Common **/
  def scala1: JValue = json"""{
    "lang": "scala",
    "year": 2006,
    "tags": ["fp", "oo"],
    "features": {
      "key1":"val1",
      "key2":"val2"
    }
  }"""

  def scala2: JValue = json"""{
    "tags": ["static-typing","fp"],
    "compiled": true,
    "lang": "scala",
    "features": {
      "key2":"newval2",
      "key3":"val3"
    }
  }"""

  def expectedMergeResult: JValue = json"""{
    "lang": "scala",
    "year": 2006,
    "tags": ["fp", "oo", "static-typing"],
    "features": {
      "key1":"val1",
      "key2":"newval2",
      "key3":"val3"
    },
    "compiled": true
  }"""

  private def lotto(x: JValue): JValue = json"""{ "lotto": $x }"""

  def lotto1: JValue = lotto(json"""{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[{
      "winner-id":23,
      "numbers":[2,45,34,23,3,5]
    }]
  }""")

  def lotto2: JValue = lotto(json"""{
    "winners":[{
      "winner-id":54,
      "numbers":[52,3,12,11,18,22]
    }]
  }""")

  def mergedLottoResult: JValue = lotto(json"""{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[{
      "winner-id":23,
      "numbers":[2,45,34,23,3,5]
    },{
      "winner-id":54,
      "numbers":[52,3,12,11,18,22]
    }]
  }""")
}
