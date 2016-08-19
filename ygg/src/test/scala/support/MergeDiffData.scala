package ygg.tests

import ygg.json.JParser._

object MergeDiffData {

  /** Diff **/
  def expectedChanges = parseUnsafe("""
    {
      "tags": ["static-typing","fp"],
      "features": {
        "key2":"newval2"
      }
    }""")

  def expectedAdditions = parseUnsafe("""
    {
      "features": {
        "key3":"val3"
      },
      "compiled": true
    }""")

  def expectedDeletions = parseUnsafe("""
    {
      "year":2006,
      "features":{ "key1":"val1" }
    }""")

  /** Common **/
  def scala1 = parseUnsafe("""
    {
      "lang": "scala",
      "year": 2006,
      "tags": ["fp", "oo"],
      "features": {
        "key1":"val1",
        "key2":"val2"
      }
    }""")

  def scala2 = parseUnsafe("""
    {
      "tags": ["static-typing","fp"],
      "compiled": true,
      "lang": "scala",
      "features": {
        "key2":"newval2",
        "key3":"val3"
      }
    }""")

  def expectedMergeResult = parseUnsafe("""
    {
      "lang": "scala",
      "year": 2006,
      "tags": ["fp", "oo", "static-typing"],
      "features": {
        "key1":"val1",
        "key2":"newval2",
        "key3":"val3"
      },
      "compiled": true
    }""")

  def lotto1 = parseUnsafe("""
    {
      "lotto":{
        "lotto-id":5,
        "winning-numbers":[2,45,34,23,7,5,3],
        "winners":[{
          "winner-id":23,
          "numbers":[2,45,34,23,3,5]
        }]
      }
    }""")

  def lotto2 = parseUnsafe("""
    {
      "lotto":{
        "winners":[{
          "winner-id":54,
          "numbers":[52,3,12,11,18,22]
        }]
      }
    }""")

  def mergedLottoResult = parseUnsafe("""
    {
      "lotto":{
        "lotto-id":5,
        "winning-numbers":[2,45,34,23,7,5,3],
        "winners":[{
          "winner-id":23,
          "numbers":[2,45,34,23,3,5]
        },{
          "winner-id":54,
          "numbers":[52,3,12,11,18,22]
        }]
      }
    }""")
}
