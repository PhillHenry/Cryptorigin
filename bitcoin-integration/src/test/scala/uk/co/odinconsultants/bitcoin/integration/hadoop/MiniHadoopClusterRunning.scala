package uk.co.odinconsultants.bitcoin.integration.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.DistributedFileSystem
import uk.co.odinconsultants.bitcoin.core.Logging
import uk.co.odinconsultants.bitcoin.integration.hadoop.HadoopForTesting.hdfsCluster

import scala.collection.mutable.ArrayBuffer

trait MiniHadoopClusterRunning extends Logging {

  val distributedFS: DistributedFileSystem  = hdfsCluster.getFileSystem
  val conf: Configuration                   = HadoopForTesting.conf
  val dir                                   = s"/${this.getClass.getSimpleName}/"

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

  def copyToHdfs(inputFile: Path): Path = {
    val fromFile  = inputFile.getName
    distributedFS.mkdirs(new Path(dir))
    val toFile    = new Path(dir + fromFile)
    info(s"Copying '$fromFile' to '$toFile' (${toFile.getName})")
    distributedFS.copyFromLocalFile(false, true, inputFile, toFile)
    toFile
  }

  def localFile(local: String): Path = {
    val classLoader = getClass.getClassLoader
    val localFQN    = classLoader.getResource(local).getFile
    new Path(localFQN)
  }
}
