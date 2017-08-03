package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Get, Table}
import uk.co.odinconsultants.bitcoin.parsing.Indexer._
import uk.co.odinconsultants.bitcoin.parsing.MetaRetrieval

class HBaseMetaRetrieval(table: Table, familyName: String) extends MetaRetrieval {

  override def apply(backReference: BackReference): PubKey = {
    val (hash, index) = backReference
    val key           = HBaseMetaStore.append(hash, index)
    val aGet          = new Get(key.array())
    table.get(aGet).value()
  }

}
