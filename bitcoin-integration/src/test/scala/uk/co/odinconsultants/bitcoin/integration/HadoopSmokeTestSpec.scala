package uk.co.odinconsultants.bitcoin.integration

import java.nio.file.Files.createTempDirectory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.MiniDFSCluster.HDFS_MINIDFS_BASEDIR
import org.scalatest.{Matchers, WordSpec}

class HadoopSmokeTestSpec extends WordSpec with Matchers {

  "Hadoop mini cluster" should {
    "be there for integration tests" in {
      val baseDir           = createTempDirectory("tests").toFile.getAbsoluteFile
      val conf              = new Configuration()
      conf.set(HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
      val builder           = new MiniDFSCluster.Builder(conf)
      val hdfsCluster       = builder.build()
      val distributedFS     = hdfsCluster.getFileSystem
      val hdfsUri           = "hdfs://127.0.0.1/" + hdfsCluster.getNameNodePort + "/"
      hdfsCluster.shutdown()

      println("Delete this test when you're satisfied everything is working fine")
    }
  }

}
