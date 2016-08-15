package com.precog

package object common {
  type AccountId    = String
  type ResetTokenId = String

  def apply(n: CPathNode*): CPath = CPath(n: _*)
}
