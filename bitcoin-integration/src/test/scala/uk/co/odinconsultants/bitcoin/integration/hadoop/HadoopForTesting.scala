package uk.co.odinconsultants.bitcoin.integration.hadoop

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.integration.utils.FsUtils.tmpDirectory

object HadoopForTesting extends Logging {

  val baseDir: File                         = tmpDirectory("tests").getAbsoluteFile
  val conf                                  = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  val builder                               = new MiniDFSCluster.Builder(conf)

  info("Attempting to start HDFS")
  val hdfsCluster: MiniDFSCluster           = builder.build()

}
