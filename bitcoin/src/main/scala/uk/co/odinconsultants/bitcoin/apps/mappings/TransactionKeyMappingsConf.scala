package uk.co.odinconsultants.bitcoin.apps.mappings

object TransactionKeyMappingsConf {

  case class KeyMappingConfig(url: String = "", refresh: Boolean = false)

  def parse(args: Array[String]): Option[KeyMappingConfig] = {
    val parser = new scopt.OptionParser[KeyMappingConfig](this.getClass.getSimpleName) {
      opt[String]('u', "url").action { (x, c) => c.copy(url = x) }.text("HDFS Url")
      opt[Unit]('r', "refresh").action{ (x, c) => c.copy(refresh = true)}.text("Refresh metadata table")
    }
    parser.parse(args, KeyMappingConfig())
  }

}
