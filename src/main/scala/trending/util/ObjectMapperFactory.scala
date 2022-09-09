package trending.util

import trending.domain.INPUT_FILE

import java.io.{File, PrintWriter}

object ObjectMapperFactory {

  private lazy val writer: PrintWriter = new PrintWriter(new File(INPUT_FILE))

  def getWriter: PrintWriter = writer
}
