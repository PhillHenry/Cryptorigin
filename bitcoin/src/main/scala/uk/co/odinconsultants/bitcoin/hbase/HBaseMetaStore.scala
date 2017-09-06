package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes.toBytes
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup.qualifier
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Batch
import uk.co.odinconsultants.bitcoin.parsing.{DomainOps, MetaStore}

import scala.collection.JavaConversions._

class HBaseMetaStore(table: Table, familyName: String) extends MetaStore with Logging {

  val familyNameAsBytes: Array[Byte] = toBytes(familyName)

  def apply(batch: Batch): Unit = {
    val puts = batch.map { case (backReference, publicKey) =>
      val (hash, index)               = backReference
      val key                         = DomainOps.append(hash, index)
      val aPut                        = new Put(key)
      aPut.addColumn(familyNameAsBytes, qualifier.getBytes, publicKey)
    }
    table.put(puts)
  }

}

