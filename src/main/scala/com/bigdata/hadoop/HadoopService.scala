package com.bigdata.hadoop

import com.bigdata.constants.HadoopConstants
import org.apache.hadoop.fs.Path

object HadoopService extends HadoopConstants with HadoopConfiguration {

  listOfFile.foreach(fileName => createFolder(fileName))

  def createFolder(folderName: String): Unit = {
    val folderPath = HADOOP_ROOT_FOLDER_PATH + "/" + folderName
    val path: Path = new Path(folderPath)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
    fileSystem.mkdirs(path)
    copyToHdfs(folderName)
  }

  def copyToHdfs(folderName: String): Unit = {
    val fileName = folderName + ".txt"
    val destPath = HADOOP_ROOT_FOLDER_PATH + "/" + folderName + "/" + fileName
    val sourcePath: String = HADOOP_SOURCE_FOLDER_PATH + "/" + fileName
    fileSystem.copyFromLocalFile(new Path(sourcePath), new Path(destPath))
  }

}
