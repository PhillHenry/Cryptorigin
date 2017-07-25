package uk.co.odinconsultants.bitcoin.hbase

import java.nio.ByteBuffer

import org.apache.hadoop.hbase.client.{HTableInterface, Put}
import org.apache.hadoop.hbase.util.Bytes.toBytes
import uk.co.odinconsultants.bitcoin.parsing.MetaStore
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Payload

import scala.Array.emptyByteArray

import scala.collection.JavaConversions._

class HBaseMetaStore(table: HTableInterface, familyName: String) extends MetaStore {

  import HBaseMetaStore._

  val familyNameAsBytes: Array[Byte] = toBytes(familyName)

  def apply(payload: Payload): Unit = {
    val puts = payload.map { case (backReference, publicKey) =>
      val (hash, index)               = backReference
      val key                         = append(hash, index)
      val aPut                        = new Put(key)
      aPut.addColumn(familyNameAsBytes, emptyByteArray, publicKey.getHash160)
    }
    table.put(puts)
  }

}

object HBaseMetaStore {

  def append(hash: Array[Byte], index: Long): ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(hash.length + 8)
    byteBuffer.put(hash)
    byteBuffer.putLong(index)
    byteBuffer.flip()
    byteBuffer
  }

}
