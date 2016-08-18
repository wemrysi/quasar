package com.precog

import quasar.ygg.table.Column

package object common {
  type AccountId    = String
  type ResetTokenId = String

  type ColumnMap = Map[ColumnRef, Column]

  def apply(n: CPathNode*): CPath = CPath(n: _*)
}
