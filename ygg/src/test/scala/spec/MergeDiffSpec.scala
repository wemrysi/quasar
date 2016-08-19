package ygg.tests

import MergeDiffData._
import ygg.json._
import JParser._

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
