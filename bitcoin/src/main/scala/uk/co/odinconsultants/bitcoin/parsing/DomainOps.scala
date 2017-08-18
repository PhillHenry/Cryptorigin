package uk.co.odinconsultants.bitcoin.parsing

import org.bitcoinj.core.ScriptException
import org.bitcoinj.core.Utils.sha256hash160
import org.bitcoinj.script.Script
import org.zuinnote.hadoop.bitcoin.format.common.BitcoinUtil.getTransactionHash
import org.zuinnote.hadoop.bitcoin.format.common._
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.parsing.Indexer.{BackReference, PubKey, networkParams}

import scala.collection.JavaConversions._

object DomainOps extends Logging {

  val toTransactions: (BitcoinBlock) => Seq[BitcoinTransaction] = _.getTransactions

  val toHash: (BitcoinBlock) => Array[Byte] = { bb =>
    BitcoinUtil.getBlockHash(bb)
  }

  val toBackReference: (BitcoinTransactionInput) => BackReference = { in =>
    (in.getPrevTransactionHash, in.getPreviousTxOutIndex)
  }

  def hashOf(tx: BitcoinTransaction): Array[Byte] = getTransactionHash(tx)

  val toOutputAddress: (BitcoinTransactionOutput) => TraversableOnce[PubKey] = { case (txOutput) =>
    val bytes = txOutput.getTxOutScript
    toPublicKey(bytes)
  }

  /**
    * @see http://codesuppository.blogspot.co.at/2014/01/how-to-parse-bitcoin-blockchain.html
    */
  def toPublicKey(bytes: Array[Byte]): Option[Array[Byte]] = {
    if (bytes.length == 67 && bytes(0) == 65) {
      Some(sha256hash160(bytes.tail.take(65)))
    } else if (bytes.length == 66) {
      Some(sha256hash160(bytes.take(65)))
    } else if (bytes.length == 5) {// "This script is in error [but] it does show up in the blockchain a number of times."
      None
    } else try {
      val script = new Script(bytes)

      if (script.isSentToAddress) Some(script.getToAddress(networkParams).getHash160)
      else if (script.isPayToScriptHash) Some(script.getToAddress(networkParams).getHash160)
      else None
    } catch {
      case x: ScriptException =>
        val msg = s"Could not convert script of length ${bytes.length}. Bytes (as hex) are ${org.apache.commons.codec.binary.Hex.encodeHex(bytes)}"
        error(msg)
        throw new ScriptException(msg, x)
    }
  }

  val toBackReferenceAddressTuples: (BitcoinTransaction) => Seq[(BackReference, PubKey)] = { tx =>
    val hash = hashOf(tx)
    tx.getListOfOutputs.zipWithIndex.flatMap { case (out, i) => toOutputAddress(out).map(addr => (hash, i.toLong) -> addr) }
  }

}
