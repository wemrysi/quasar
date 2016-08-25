package ygg.table

import scalaz._
import ygg._, common._, json._, trans._

trait TableModule {
  val Table: TableCompanion

  type Table <: TableLike
  type TableCompanion <: ygg.table.TableCompanion


  trait TableLike extends ygg.table.Table {
    this: Table =>

    /**
      * For each distinct path in the table, load all columns identified by the specified
      * jtype and concatenate the resulting slices into a new table.
      */
    def load(apiKey: APIKey, tpe: JType): Need[Table]

    /**
      * Removes all rows in the table for which definedness is satisfied
      * Remaps the indicies.
      */
    def compact(spec: TransSpec1, definedness: Definedness = AnyDefined): Table

    /**
      * Performs a one-pass transformation of the keys and values in the table.
      * If the key transform is not identity, the resulting table will have
      * unknown sort order.
      */
    def transform(spec: TransSpec1): Table

    /**
      * Cogroups this table with another table, using equality on the specified
      * transformation on rows of the table.
      */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table

    /**
      * Performs a full cartesian cross on this table with the specified table,
      * applying the specified transformation to merge the two tables into
      * a single table.
      */
    def cross(that: Table)(spec: TransSpec2): Table

    /**
      * Sorts the KV table by ascending or descending order of a transformation
      * applied to the rows.
      *
      * @param sortKey The transspec to use to obtain the values to sort on
      * @param sortOrder Whether to sort ascending or descending
      * @param unique If true, the same key values will sort into a single row, otherwise
      * we assign a unique row ID as part of the key so that multiple equal values are
      * preserved
      */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): M[Table]

    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table] = sort(sortKey, sortOrder, unique = false)
    def sort(sortKey: TransSpec1): M[Table]                              = sort(sortKey, SortAscending)

    def distinct(spec: TransSpec1): Table

    def concat(t2: Table): Table

    def zip(t2: Table): M[Table]

    /**
      * Sorts the KV table by ascending or descending order based on a seq of transformations
      * applied to the rows.
      *
      * @param groupKeys The transspecs to use to obtain the values to sort on
      * @param valueSpec The transspec to use to obtain the non-sorting values
      * @param sortOrder Whether to sort ascending or descending
      * @param unique If true, the same key values will sort into a single row, otherwise
      * we assign a unique row ID as part of the key so that multiple equal values are
      * preserved
      */
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): M[Seq[Table]]

    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table]
  }
}
