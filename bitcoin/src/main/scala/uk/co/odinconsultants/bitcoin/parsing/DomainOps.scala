package uk.co.odinconsultants.bitcoin.parsing

import java.nio.ByteBuffer

import org.bitcoinj.core.ScriptException
import org.bitcoinj.core.Utils.sha256hash160
import org.bitcoinj.script.Script
import org.bitcoinj.script.ScriptOpCodes._
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil.getTransactionHash
import org.zuinnote.hadoop.bitcoin.format.common._
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{BackReference, PubKey, networkParams}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object DomainOps extends Logging {

  val toTransactions: (BitcoinBlock) => Seq[BitcoinTransaction] = _.getTransactions

  val toHash: (BitcoinBlock) => Array[Byte] = { bb =>
    BitcoinUtil.getBlockHash(bb)
  }

  val toBackReference: (BitcoinTransactionInput) => BackReference = { in =>
    (in.getPrevTransactionHash, in.getPreviousTxOutIndex)
  }

  def appendAndSalt(hash: Array[Byte], index: Long): ByteBuffer = {
    val byteBuffer: ByteBuffer = append(hash, index)

    import java.security.MessageDigest
    val digest    = MessageDigest.getInstance("SHA-256")
    val digested  = digest.digest(byteBuffer.array())

    ByteBuffer.wrap(digested)
  }

  private[parsing] def append(hash: Array[Byte], index: Long) = {
    val byteBuffer = ByteBuffer.allocate(hash.length + 8)
    byteBuffer.put(hash)
    byteBuffer.putLong(index)
    byteBuffer.flip()
    byteBuffer
  }

  def hashOf(tx: BitcoinTransaction): Array[Byte] = getTransactionHash(tx)

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val bytes = txOutput.getTxOutScript
    toPublicKey(bytes)
  }

  /**
    * @see http://codesuppository.blogspot.co.at/2014/01/how-to-parse-bitcoin-blockchain.html
    * @see https://bitcointalk.org/index.php?topic=1847290.0
    */
  def toPublicKey(bytes: Array[Byte]): Option[Array[Byte]] = {
    if (bytes.length < 23) {
      // John Ratcliff says if the length of the script is 5,
      // "This script is in error [but] it does show up in the blockchain a number of times."
      // but I see other amounts too. No idea why.
      error(s"Hmm. Script length of ${bytes.length}. Content as hex = ${toHex(bytes)}")
      None
    } else if (bytes.length == 67 && bytes(0) == 65) {
      Some(sha256hash160(bytes.tail.take(65)))
    } else if (bytes.length == 66) {
      Some(sha256hash160(bytes.take(65)))
    } else if (beginsSensibly(bytes) && bytes(23) == OP_EQUALVERIFY && bytes(24) == OP_CHECKSIG) {
      Some(sha256hash160(bytes.slice(3, 23)))
//    } else if (beginsSensibly(bytes)) { // not sure about this as the source (John Ratcliff) is somewhat inconsistent
//      Some(sha256hash160(bytes.slice(3, 23)))
    } else {
      Try {
        val script = new Script(bytes)
        Some(script.getToAddress(networkParams).getHash160)
      } match {
        case Failure(x) =>
          error(s"BitcoinJ could not parse ${toHex(bytes)}. Error = ${x.getMessage}")
          None
        case Success(x) => x
      }
    }
  }

  def beginsSensibly(bytes: Array[Byte]): Boolean = bytes(0) == OP_DUP && bytes(1) == OP_HASH160 && bytes(2) == 20

  def toHex(bytes: Array[Byte]): String = new String(org.apache.commons.codec.binary.Hex.encodeHex(bytes))

  val toBackReferenceAddressTuples: (BitcoinTransaction) => Seq[(BackReference, PubKey)] = { tx =>
    val hash = hashOf(tx)
    tx.getListOfOutputs.zipWithIndex.flatMap { case (out, i) => toOutputAddress(out).map(addr => (hash, i.toLong) -> addr) }
  }

}
