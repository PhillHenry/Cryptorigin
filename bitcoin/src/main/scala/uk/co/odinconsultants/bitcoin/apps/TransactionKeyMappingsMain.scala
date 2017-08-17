package uk.co.odinconsultants.bitcoin.apps

import uk.co.odinconsultants.bitcoin.core.Logging

object TransactionKeyMappingsMain extends Logging {

  import TransactionKeyMappingsConf._

  def main(args: Array[String]): Unit = {
    parse(args) match {
      case Some(c)  => process(c)
      case None     => error(s"Cannot parse ${args.mkString(", ")}")
    }
  }

  def process(config: KeyMappingConfig): Unit = {

  }

}
