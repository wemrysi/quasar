package quasar
package fs

import quasar.Predef.String

// TODO: Rename to [[PathError]] once we've deprecated the other [[Path]] type.
sealed trait PathError2 {
  def message: String
}

// TODO: Cases.

