/*
 * Copyright 2009-2010 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package blueeyes
package json

import MergeDiffData._
import JParser._
// import quasar.precog.TestSupport._

class MergeDiffSpec extends quasar.Qspec {
  "Merge example" in {
    (scala1 merge scala2) mustEqual expectedMergeResult
  }

  "Lotto example" in {
    (lotto1 merge lotto2) mustEqual mergedLottoResult
  }

  "Diff example" in {
    val Diff(changed, added, deleted) = scala1 diff scala2

    changed mustEqual expectedChanges
    added mustEqual expectedAdditions
    deleted mustEqual expectedDeletions
  }

  "Lotto example" in {
    val Diff(changed, added, deleted) = mergedLottoResult diff lotto1
    changed mustEqual JUndefined
    added mustEqual JUndefined
    deleted mustEqual lotto2
  }

  "Example from http://tlrobinson.net/projects/js/jsondiff/" in {
    val json1             = read("/diff-example-json1.json")
    val json2             = read("/diff-example-json2.json")
    val expectedChanges   = read("/diff-example-expected-changes.json")
    val expectedAdditions = read("/diff-example-expected-additions.json")
    val expectedDeletions = read("/diff-example-expected-deletions.json")

    val Diff(changes, additions, deletions) = json1 diff json2

    changes.renderCanonical mustEqual expectedChanges.renderCanonical
    additions.renderCanonical mustEqual expectedAdditions.renderCanonical
    deletions.renderCanonical mustEqual expectedDeletions.renderCanonical
  }

  private def read(resource: String) = parseUnsafe(scala.io.Source.fromInputStream(getClass.getResourceAsStream(resource)).getLines.mkString)
}

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
