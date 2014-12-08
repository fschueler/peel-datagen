package eu.stratosphere.peel.datagen.spark

import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.mllib.random.{UniformGenerator, RandomRDDs}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/** Generates tuple-data for Peel experiments using Spark.
  *
  * This datagenerator can be used in Peel's [[eu.stratosphere.peel.core.beans.data.GeneratedDataSet GeneratedDataSet]] to
  * generate data for experiments that make use of key-value tuples.
  *
  */
object SparkTupleGenerator {

  /** Distribution Patterns to parse string distribution-arguments */
  object Distributions {
    val Uniform = "Uniform"
    val Normal = "Gaussian"
    val Pareto = "Pareto"
  }

  object Command {
    // argument names
    val KEY_N = "N"
    val KEY_DOP = "dop"
    val KEY_OUTPUT = "output"
    val KEY_KEYDIST = "key-distribution"
    val KEY_PAYLOAD = "payload"
    val KEY_UPPER_BOUND = "upper"
    val KEY_LOWER_BOUND = "lower"
  }

  class Command extends SparkDataGenerator.Command[SparkTupleGenerator]() {

    // algorithm names
    override def name = "TupleGenerator"

    override def description = "Generate a dataset that consists of Tuples (K, V, A)"

    override def setup(parser: Subparser): Unit = {
      super.setup(parser)

      // add arguments

      parser.addArgument(Command.KEY_DOP)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_DOP)
        .metavar("DOP")
        .help("degree of parallelism")
      parser.addArgument(Command.KEY_N)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_N)
        .metavar("N")
        .help("number of points to generate")
      parser.addArgument(Command.KEY_UPPER_BOUND)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_UPPER_BOUND)
        .metavar("L")
        .help("upper bound for the generated distribution")
      parser.addArgument(Command.KEY_LOWER_BOUND)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_LOWER_BOUND)
        .metavar("U")
        .help("lower bound for the generated distribution")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file ")
      parser.addArgument(Command.KEY_PAYLOAD)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_PAYLOAD)
        .metavar("PAYLOAD")
        .help("length of the string value")
      parser.addArgument(Command.KEY_KEYDIST)
        .`type`[String](classOf[String])
        .dest(Command.KEY_KEYDIST)
        .metavar("DISTRIBUTION")
        .help("distribution to use for the keys")
    }
  }

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- Schema -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {

    case class KV(key: Int, value: String, aggregation: Int) {
      override def toString = s"$key,$value,$aggregation"
    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      throw new RuntimeException("Arguments count != 8")
    }

    try {
      val master: String = args(0)
      val numTasks: Int = args(1).toInt
      val tuplesPerTask: Int = args(2).toInt
      val lower = args(3).toInt
      val upper = args(4).toInt
      val keyDist: String =  validDistribution(args(5))
      val pay: Int = args(6).toInt
      val output: String = args(7)

      val generator = new SparkTupleGenerator(master, numTasks, tuplesPerTask, lower, upper, keyDist, pay, output)
      generator.run()

    } catch {
      case e: Exception => {
        println("Invalid arguments for tuple generator")
        println(e)
        throw new IllegalStateException("Invalid arguments for tuple generator", e)
      }
    }
  }

  def validDistribution(s: String): String = s match {
    case d@Distributions.Pareto => d
    case d@Distributions.Normal => d
    case d@Distributions.Uniform => d
    case _ => throw new IllegalArgumentException("No valid distribution. Available are: Uniform, Normal, Pareto")
  }
}

/** Generator for synthetic, tuple-data running on Spark.
  *
  * Generates tuples of the form (key: Int, value: String, aggregation: Int) where the distribution for
  * the keys and aggregation-values can be chosen from one out of three distributions:
  * $ Uniform(a, b)
  * $ Gaussian(sigma, mu)
  * $ Pareto(a)
 *
 * @param master Spark master
 * @param numTasks Degree of parallelism
 * @param tuplesPerTask Number of tuples per task to generate
 * @param lowerBound Lower bound for the generated values
 * @param upperBound Upper bound for the generated values
 * @param keyDist Distribution for the Key
 * @param pay Length of the payload-string
 * @param output Path for output-tuples
 */
class SparkTupleGenerator(master: String, numTasks: Int, tuplesPerTask: Int, lowerBound: Int, upperBound: Int, keyDist: String, pay: Int, output: String) extends SparkDataGenerator(master) {

  import eu.stratosphere.peel.datagen.spark.SparkTupleGenerator.Schema.KV

  def this(ns: Namespace) = this(

    ns.get[String](SparkDataGenerator.Command.KEY_MASTER),
    ns.get[Int](SparkTupleGenerator.Command.KEY_DOP),
    ns.get[Int](SparkTupleGenerator.Command.KEY_N),
    ns.get[Int](SparkTupleGenerator.Command.KEY_LOWER_BOUND),
    ns.get[Int](SparkTupleGenerator.Command.KEY_UPPER_BOUND),
    SparkTupleGenerator.validDistribution(ns.get[String](SparkTupleGenerator.Command.KEY_KEYDIST)),
    ns.get[Int](SparkTupleGenerator.Command.KEY_PAYLOAD),
    ns.get[String](SparkTupleGenerator.Command.KEY_OUTPUT))

  def run() = {
    val conf = new SparkConf().setAppName(new SparkTupleGenerator.Command().name).setMaster(master)
    val sc = new SparkContext(conf)

    val N: Long = tuplesPerTask.toLong * numTasks // number of points generated in total

    val payload = new Random(SEED).nextString(pay)
    val aggValuesGenerator = new UniformGenerator()
    aggValuesGenerator.setSeed(SEED)

    val randomRDD = keyDist match {
      case SparkTupleGenerator.Distributions.Uniform => {
        val lower = lowerBound
        val upper = upperBound
        RandomRDDs.uniformRDD(sc, N, numTasks, this.SEED).map{ v =>
          val key = (lower + (upper - lower) * v).toInt
          val value = (aggValuesGenerator.nextValue * 1000).toInt
          KV(key, payload, value)
        }
      }

      case SparkTupleGenerator.Distributions.Normal => {

        // set deviation to +/- 3 standard deviations, times half the range
        val halfRange = (upperBound - lowerBound) / 2.0
        val mean = lowerBound + halfRange
        val deviation = halfRange / 3.0

        RandomRDDs.normalRDD(sc, N, numTasks, this.SEED).map { v =>
          //val key = (((Math.sqrt(2.5) * v) * 100.0) + 500).toInt
          val key = ((v * deviation) + mean).toInt
          val value = (aggValuesGenerator.nextValue * 1000).toInt
          KV(key, payload, value)
        }
      }

      case SparkTupleGenerator.Distributions.Pareto => {
        if (lowerBound <= 0)
          throw new IllegalArgumentException("Lower bound for pareto distribution has to be > 0")

        val shape = 0.01 // alpha
        val uP = Math.pow(upperBound,shape)
        val lP = Math.pow(lowerBound,shape)

        RandomRDDs.uniformRDD(sc, N, numTasks, this.SEED).map{ v =>
          val term = -1 * ((v * uP -  v * lP - uP) / (uP * lP))
          val key  = Math.pow(term,(-1 / shape)).toInt

          val value = (aggValuesGenerator.nextValue * 1000).toInt

          KV(key, payload, value)
        }
      }
    }

    randomRDD.saveAsTextFile(output)

    sc.stop()
  }
}
