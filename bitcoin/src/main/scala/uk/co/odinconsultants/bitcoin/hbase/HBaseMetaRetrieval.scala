package uk.co.odinconsultants.bitcoin.hbase

import org.apache.hadoop.hbase.client.{Get, HTableInterface}
import org.bitcoinj.core.Address
import uk.co.odinconsultants.bitcoin.parsing.Indexer._
import uk.co.odinconsultants.bitcoin.parsing.MetaRetrieval

class HBaseMetaRetrieval(table: HTableInterface, familyName: String) extends MetaRetrieval {

  import HBaseMetaRetrieval._

  override def apply(backReference: BackReference): PubKey = {
    val (hash, index) = backReference
    val key           = HBaseMetaStore.append(hash, index)
    val aGet          = new Get(key.array())
    val result        = table.get(aGet)
    val address       = toAddress(result.value)
    address
  }

}

object HBaseMetaRetrieval {

  def toAddress(result: Array[Byte]): Address = Address.fromP2SHHash(networkParams, result)

}