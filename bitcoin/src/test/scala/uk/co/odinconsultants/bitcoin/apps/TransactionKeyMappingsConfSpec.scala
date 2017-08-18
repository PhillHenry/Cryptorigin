package uk.co.odinconsultants.bitcoin.apps

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.apps.mappings.TransactionKeyMappingsConf.{KeyMappingConfig, parse}

class TransactionKeyMappingsConfSpec extends WordSpec with Matchers {

  "URL argument" should {
    "be parsed" in {
      val url = "a_url"
      val configOpt = parse(Array("-u", url))
      configOpt shouldBe Some(KeyMappingConfig().copy(url = url))
    }
  }

  "Refresh argument" should {
    "be parsed" in {
      val configOpt = parse(Array("-r"))
      configOpt shouldBe Some(KeyMappingConfig().copy(refresh = true))
    }
    "default to false" in {
      KeyMappingConfig().refresh shouldBe false
    }
  }

}
