package uk.co.odinconsultants.bitcoin.apps

object TransactionKeyMappingsConf {

  case class KeyMappingConfig(url: String = "")

  def parse(args: Array[String]): Option[KeyMappingConfig] = {
    val parser = new scopt.OptionParser[KeyMappingConfig](this.getClass.getSimpleName) {
      opt[String]('u', "url").action { (x, c) => c.copy(url = x) }.text("HDFS Url")
    }
    parser.parse(args, KeyMappingConfig())
  }

}
