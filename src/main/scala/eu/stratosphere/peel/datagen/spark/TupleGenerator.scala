package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.util.Distributions._
import eu.stratosphere.peel.datagen.util.{DatasetGenerator, RanHash}
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object TupleGenerator {

  object Patterns {
    val Uniform = "\\bUniform\\(\\d\\)".r
    val Gaussian = "\\bGaussian\\(\\d,\\d\\)".r
    val Pareto = "\\bPareto\\(\\d\\)".r
  }
  object Command {
    // argument names
    val KEY_N = "N"
    val KEY_DOP = "dop"
    val KEY_OUTPUT = "output"
    val KEY_KEYDIST = "key-distribution"
    val KEY_AGGDIST = "aggregate-distribution"
    val KEY_PAYLOAD = "payload"
  }

  class Command extends Algorithm.Command[TupleGenerator]() {

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
      parser.addArgument(Command.KEY_AGGDIST)
        .`type`[String](classOf[String])
        .dest(Command.KEY_AGGDIST)
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
    if (args.length != 7) {
      throw new RuntimeException("Arguments count !- 7")
    }

    val master: String = args(0)
    val dop: Int = args(1).toInt
    val N: Int = args(2).toInt
    val output: String = args(3)
    val keyDist: Distribution = parseDist(args(4))
    val pay: Int = args(5).toInt
    val aggDist: Distribution = parseDist(args(6))

    val generator = new TupleGenerator(master, dop, N, output, keyDist, pay, aggDist)
    generator.run()
  }

  def parseDist(s: String): Distribution = s match {
    case Patterns.Pareto(a) => Pareto(a.toDouble)
    case Patterns.Gaussian(a,b) => Gaussian(a.toDouble, b.toDouble)
    case Patterns.Uniform(a) => Uniform(a.toInt)
    case _ => Uniform(10)
  }
}

class TupleGenerator(master: String, dop: Int, N: Int, output: String, keyDist: Distribution, pay: Int, aggDist: Distribution) extends Algorithm(master) with DatasetGenerator {

  import eu.stratosphere.peel.datagen.spark.TupleGenerator.Schema.KV

  def this(ns: Namespace) = this(
    ns.get[String](Algorithm.Command.KEY_MASTER),
    ns.get[Int](TupleGenerator.Command.KEY_DOP),
    ns.get[Int](TupleGenerator.Command.KEY_N),
    ns.get[String](TupleGenerator.Command.KEY_OUTPUT),
    TupleGenerator.parseDist(ns.get[String](TupleGenerator.Command.KEY_KEYDIST)),
    ns.get[Int](TupleGenerator.Command.KEY_PAYLOAD),
    TupleGenerator.parseDist(ns.get[String](TupleGenerator.Command.KEY_AGGDIST)))

  def run() = {
    val conf = new SparkConf().setAppName(new TupleGenerator.Command().name).setMaster(master)
    val sc = new SparkContext(conf)

    val n = N / dop - 1 // number of points generated in each partition
    val s = new Random(SEED).nextString(pay)
    val seed = this.SEED

    val kd = this.keyDist
    val ag = this.aggDist


    val dataset = sc.parallelize(0 until dop, dop).flatMap(i => {
      val partitionStart = n * i // the index of the first point in the current partition
      val randStart = partitionStart * 2 // the start for the prng (time 2 because we need 2 numbers for each tuple)
      val rand = new RanHash(seed)
      rand.skipTo(seed + randStart)

      for (j <- partitionStart to (partitionStart + n)) yield {
        KV(keyDist.sample(rand).toInt, s, aggDist.sample(rand).toInt)
      }
    })

    dataset.saveAsTextFile(output)
    sc.stop()
  }
}
