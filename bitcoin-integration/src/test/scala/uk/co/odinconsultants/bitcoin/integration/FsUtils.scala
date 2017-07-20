package uk.co.odinconsultants.bitcoin.integration

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils

object FsUtils {

  def tmpDirectory(path: String): File = {
    val file = Files.createTempDirectory(path).toFile
    FileUtils.forceDeleteOnExit(file)
    file
  }

}
