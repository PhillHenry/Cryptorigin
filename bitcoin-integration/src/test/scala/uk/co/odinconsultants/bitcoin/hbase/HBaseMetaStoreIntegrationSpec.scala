package uk.co.odinconsultants.bitcoin.hbase

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.bitcoin.hbase.HBaseSetup._
import uk.co.odinconsultants.bitcoin.integration.hbase.HBaseRunning
import uk.co.odinconsultants.bitcoin.integration.utils.DomainTestObjects._

@RunWith(classOf[JUnitRunner])
class HBaseMetaStoreIntegrationSpec extends WordSpec with Matchers with HBaseRunning {

  "HBase" should {
    "be there for integration tests" in {
      createAddressesTable(admin)
      val connection      = admin.getConnection
      val table           = connection.getTable(metaTable)

      val inserter        = new HBaseMetaStore(connection.getTable(metaTable), familyName)
      val metadatum       = ((hash, index), expectedAddress)
      val metadata        = List(metadatum)
      inserter(metadata)

      val selector        = new HBaseMetaRetrieval(table, familyName)
      val actualAddress   = selector(hash, index)
      actualAddress shouldEqual expectedAddress
    }
  }

}
