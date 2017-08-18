package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Get, Table}
import uk.co.odinconsultants.bitcoin.parsing.Indexer._
import uk.co.odinconsultants.bitcoin.parsing.{DomainOps, MetaRetrieval}

import scala.collection.JavaConversions._

class HBaseMetaRetrieval(table: Table, familyName: String) extends MetaRetrieval {

  override def apply(batch: List[BackReference]): List[PubKey] = {
    val gets = batch map {  backReference =>
      val (hash, index) = backReference
      val key           = DomainOps.append(hash, index)
      new Get(key.array())
    }
    table.get(gets).map(_.value()).toList
  }

}
