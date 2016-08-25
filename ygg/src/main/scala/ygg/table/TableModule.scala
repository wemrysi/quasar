package ygg.table

trait TableModule {
  outer =>

  val Table: TableCompanion
  type Table <: ygg.table.Table
  type TableCompanion <: ygg.table.TableCompanion[Table]
}
