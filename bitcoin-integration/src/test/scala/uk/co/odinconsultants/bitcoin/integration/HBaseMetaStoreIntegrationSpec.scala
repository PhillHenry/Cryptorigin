package uk.co.odinconsultants.bitcoin.integration

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup._
import uk.co.odinconsultants.bitcoin.hbase.{HBaseMetaRetrieval, HBaseMetaStore}
import uk.co.odinconsultants.bitcoin.integration.DomainTestObjects._

@RunWith(classOf[JUnitRunner])
class HBaseMetaStoreIntegrationSpec extends WordSpec with Matchers with HBaseTesting {

  "HBase" should {
    "be there for integration tests" in {
      createAddressesTable(admin)
      val table           = admin.getConnection.getTable(tableName)

      val inserter        = new HBaseMetaStore(table, familyName)
      inserter((hash, index), expectedAddress)

      val selector        = new HBaseMetaRetrieval(table, familyName)
      val actualAddress   = selector(hash, index)
      actualAddress shouldEqual expectedAddress

      utility.shutdownMiniCluster()
    }
  }

}
