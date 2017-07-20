package uk.co.odinconsultants.bitcoin.integration

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{DistributedFileSystem, MiniDFSCluster}
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.integration.FsUtils.tmpDirectory

import scala.collection.mutable.ArrayBuffer

trait MiniDfsClusterRunning extends Logging {

  val baseDir: File                         = tmpDirectory("tests").getAbsoluteFile
  val conf                                  = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  val builder                               = new MiniDFSCluster.Builder(conf)

  info("Attempting to start HDFS")
  val hdfsCluster: MiniDFSCluster           = builder.build()
  val distributedFS: DistributedFileSystem  = hdfsCluster.getFileSystem
  val hdfsUri: String                       = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"

  def list(path: String): List[Path] = {
    info(s"Looking in $path")

    val files = distributedFS.listFiles(new Path(path), true)

    val allPaths = ArrayBuffer[Path]()
    while (files.hasNext) {
      val file = files.next
      allPaths += file.getPath
    }

    allPaths.toList
  }

  def copy(local: String, hdfs: String): Unit = {
    val classLoader = getClass.getClassLoader
    val localFQN    = classLoader.getResource(local).getFile
    val inputFile   = new Path(localFQN)
    distributedFS.copyFromLocalFile(false, false, inputFile, new Path(hdfs))
  }


}
