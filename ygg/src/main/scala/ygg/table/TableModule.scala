package ygg.table

trait TableModule {
  val Table: TableCompanion
  type Table <: ygg.table.Table
  type TableCompanion <: ygg.table.TableCompanion

  trait TableLike extends ygg.table.Table
}
