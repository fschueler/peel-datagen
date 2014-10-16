package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.util.RanHash
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.{SparkConf, SparkContext}

object SparkClusterGenerator {

  object Command {
    // argument names
    val KEY_N = "N"
    val KEY_DOP = "dop"
    val KEY_OUTPUT = "output"
    val KEY_INPUT = "input"
  }

  class Command extends SparkDataGenerator.Command[SparkClusterGenerator]() {

    // algorithm names
    override def name = "ClusterGenerator"

    override def description = "Generate a dataset that consists of k clusters"

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
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("INPUT")
        .help("input file holding the cluster centers")
    }
  }

  // --------------------------------------------------------------------------------------------
  // ----------------------------------- Schema -------------------------------------------------
  // --------------------------------------------------------------------------------------------

  object Schema {

    case class Point(id: Int, clusterID: Int, vec: Array[Double]) {
      override def toString = s"$id, $clusterID, ${vec.mkString(",")}"
    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      throw new RuntimeException("Arguments count != 5")
    }

    val master: String = args(0)
    val dop: Int = args(0).toInt
    val N: Int = args(0).toInt
    val output: String = args(0)
    val input: String = args(0)

    val generator = new SparkClusterGenerator(master, dop, N, output, input)
    generator.run()
  }
}

class SparkClusterGenerator(master: String, dop: Int, N: Int, output: String, input: String) extends SparkDataGenerator(master) {

  import eu.stratosphere.peel.datagen.spark.SparkClusterGenerator.Schema.Point

  def this(ns: Namespace) = this(
    ns.get[String](SparkDataGenerator.Command.KEY_MASTER),
    ns.get[Int](SparkClusterGenerator.Command.KEY_DOP),
    ns.get[Int](SparkClusterGenerator.Command.KEY_N),
    ns.get[String](SparkClusterGenerator.Command.KEY_OUTPUT),
    ns.get[String](SparkClusterGenerator.Command.KEY_INPUT))

  def run() = {
    val conf = new SparkConf().setAppName(new SparkClusterGenerator.Command().name).setMaster(master)
    val sc = new SparkContext(conf)

    // cluster centers
    val csv = sc.textFile(input).map { line =>
      line.split(",").map(_.toDouble)
    }.collect()

    val n = N / dop - 1 // number of points generated in each partition
    val K = csv.size
    val ppc = N / K // number of points per center
    val tDim = csv.head.drop(2).size
    val seed = this.SEED

    val centroids = sc.broadcast(csv)

    val dataset = sc.parallelize(0 until dop, dop).flatMap(i => {
      val partitionStart = n * i // the index of the first point in the current partition
      val randStart = partitionStart * (tDim + 1) // the start for the prng: one points requires tDim + randoms
      val rand = new RanHash(seed)
      rand.skipTo(seed + randStart)

      for (j <- partitionStart to (partitionStart + n)) yield {
        val centroidID = rand.nextInt(K)
        val centroid = centroids.value(centroidID)
        val id = centroid(0).toInt
        val sigma = centroid(1)
        val vec = for (x <- centroid.drop(2)) yield x + sigma * rand.nextGaussian() // generate random number for evey dimension
        Point(j, id, vec)
      }
    })

    dataset.saveAsTextFile(output)
    sc.stop()
  }
}

