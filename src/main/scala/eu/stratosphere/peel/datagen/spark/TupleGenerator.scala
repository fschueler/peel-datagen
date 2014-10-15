package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.util.Distributions._
import eu.stratosphere.peel.datagen.util.{DatasetGenerator, RanHash}
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object TupleGenerator {

  object Command {
    // argument names
    val KEY_N = "N"
    val KEY_DOP = "dop"
    val KEY_OUTPUT = "output"
    val KEY_DIST = "distribution"
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

      val subparser = parser.addSubparsers()
      subparser.addParser("dist")
        //.addArgument(Command.KEY_NORM)
        //.`type`[String](classOf[String])
        //.dest(Command.KEY_DIST)
        //.metavar("DISTRIBUTION")
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
    val dop: Int = args(0).toInt
    val N: Int = args(0).toInt
    val output: String = args(0)
    val keyDist: Distribution = Pareto(1) // TODO
    val pay: Int = args(0).toInt
    val aggDist: Int = args(0).toInt

    val generator = new TupleGenerator(master, dop, N, output, keyDist, pay)
    generator.run()
  }
}

class TupleGenerator(master: String, dop: Int, N: Int, output: String, keyDist: Distribution, pay: Int, aggDist: Distribution = Uniform(0, 10)) extends Algorithm(master) with DatasetGenerator {

  import eu.stratosphere.peel.datagen.spark.TupleGenerator.Schema.KV

  def this(ns: Namespace) = this(
    ns.get[String](Algorithm.Command.KEY_MASTER),
    ns.get[Int](TupleGenerator.Command.KEY_DOP),
    ns.get[Int](TupleGenerator.Command.KEY_N),
    ns.get[String](TupleGenerator.Command.KEY_OUTPUT),
    ns.get[Distribution](TupleGenerator.Command.KEY_DIST),
    ns.get[Int](TupleGenerator.Command.KEY_PAYLOAD))

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

      def randomKey(): Int = kd match {
        case Gaussian(mus, sigma) => (sigma * rand.nextGaussian()).toInt
        case Pareto(a) => rand.nextPareto(a).toInt
        case Uniform(a, b) => rand.nextInt(b) - a
      }

      def randomAggregate(): Int = ag match {
        case Gaussian(mus, sigma) => (sigma * rand.nextGaussian()).toInt
        case Pareto(a) => rand.nextPareto(a).toInt
        case Uniform(a, b) => rand.nextInt(b)
      }

      for (j <- partitionStart to (partitionStart + n)) yield {
        KV(randomKey(), s, randomAggregate())
      }
    })

    dataset.saveAsTextFile(output)
    sc.stop()
  }
}
