package uk.co.odinconsultants.bitcoin.integration

import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.bitcoinj.core.Address
import uk.co.odinconsultants.bitcoin.hbase.HBaseMetaRetrieval.toAddress

object DomainTestObjects {

  val hash: Array[Byte]         = toBytes("rowkey1")
  val index: Long               = 42
  val rawAddress: Array[Byte]   = Array.fill(20)(0.toByte)
  val expectedAddress: Address  = toAddress(rawAddress)

}
