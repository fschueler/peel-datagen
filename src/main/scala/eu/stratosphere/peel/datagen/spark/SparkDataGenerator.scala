package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.DataGenerator
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

/** Represents a data-generator class for synthetic data that can be used in Peel's
  * [[eu.stratosphere.peel.core.beans.data.GeneratedDataSet GeneratedDataSet]].
 *
 */
object SparkDataGenerator {

  object Command {
    // argument names
    val KEY_MASTER = "master"
  }

  abstract class Command[A <: SparkDataGenerator](implicit m: scala.reflect.Manifest[A]) extends DataGenerator.Command {

    override def setup(parser: Subparser): Unit = {
      // add parameters
      parser.addArgument(s"--${Command.KEY_MASTER}")
        .`type`[String](classOf[String])
        .dest(Command.KEY_MASTER)
        .metavar("URL")
        .help("Spark master (default: local[*])")

      parser.setDefault(Command.KEY_MASTER, "local[*]")
    }
  }

}

abstract class SparkDataGenerator(val sparkMaster: String) extends DataGenerator {

  def this(ns: Namespace) = this(ns.get[String](SparkDataGenerator.Command.KEY_MASTER))
}

