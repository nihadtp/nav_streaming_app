package org.mutualFund.utils

import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.{
  ArgumentParser,
  ArgumentParserException,
  Namespace
}

object argsUtil {
  val parser: ArgumentParser = ArgumentParsers
    .newFor("Prodigal Spark Application")
    .build()
    .defaultHelp(true)
    .description("Pass config file path via command line argument.")

  parser
    .addArgument("-c", "--config")
    .nargs(1)
    .help("Absolute path for the config file")

  def getNameSpace(args: Array[String]): Namespace = {
    var ns: Namespace = null
    try {
      ns = parser.parseArgs(args)
    } catch {
      case e: ArgumentParserException =>
        parser.handleError(e)
        System.exit(1)
    }
    ns
  }
}
