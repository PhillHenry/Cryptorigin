package uk.co.odinconsultants.bitcoin.hbase

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseForTesting.admin

class HBaseSetupIntegrationSpec extends WordSpec with Matchers {

  import HBaseSetup._

  val testTableName: String = System.currentTimeMillis().toString

  "A table that we have not created" should {
    "not exist" in {
      tableExists(testTableName, admin) shouldBe false
    }
  }

  "Creating and dropping a table" should {
    "first indicate it exists" in {
      createTable(admin, toTableName(testTableName), familyName)
      tableExists(testTableName, admin) shouldBe true
    }
    "then afterwards, says it doesn't" in {
      dropTable(testTableName, admin)
      tableExists(testTableName, admin) shouldBe false
    }
  }

  "Calling prepare table twice" should {
    "not be a problem" in {
      prepareMetaTable(refresh = true, admin.getConnection)
      tableExists(metaTable, admin) shouldBe true
      prepareMetaTable(refresh = true, admin.getConnection)
      tableExists(metaTable, admin) shouldBe true
    }
  }

}
