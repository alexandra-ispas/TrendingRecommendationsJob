package trending.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FilesManager {

  val fs: FileSystem = FileSystem.get(new Configuration())

  def deleteFile(filename: String): Unit = {
    fs.listStatus(new Path("."))
      .filter(x => filename.r.findFirstIn(x.getPath.getName).isDefined)
      .foreach(x => {
        val filePath = new Path("./" + x.getPath.getName)
        if (fs.exists(filePath))
          fs.delete(filePath, true)
      })
  }

}
