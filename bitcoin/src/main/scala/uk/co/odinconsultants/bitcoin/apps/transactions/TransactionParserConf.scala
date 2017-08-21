package uk.co.odinconsultants.bitcoin.apps.transactions

object TransactionParserConf {

  case class TransactionParserConfig(inputUrl: String = "", outputUrl: String = "")

  def parse(args: Array[String]): Option[TransactionParserConfig] = {
    val parser = new scopt.OptionParser[TransactionParserConfig](this.getClass.getSimpleName) {
      opt[String]('i', "inputUrl").action { (x, c) => c.copy(inputUrl = x) }.text("HDFS Input Url")
      opt[String]('o', "outputUrl").action{ (x, c) => c.copy(outputUrl = x)}.text("HDFS Output Url")
    }
    parser.parse(args, TransactionParserConfig())
  }

}
