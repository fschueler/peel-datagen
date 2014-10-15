package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.util.RanHash
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}
import org.apache.spark.{SparkConf, SparkContext}

object ClusterGenerator {

  object Command {
    // argument names
    val KEY_N = "N"
    val KEY_DOP = "dop"
    val KEY_OUTPUT = "output"
    val KEY_INPUT = "input"
    val KEY_K = "k"
    val KEY_DIM = "dim"
  }

  class Command extends Algorithm.Command[ClusterGenerator]() {

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
      parser.addArgument(Command.KEY_K)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_K)
        .metavar("K")
        .help("number of clusters")
      parser.addArgument(Command.KEY_DIM)
        .`type`[Int](classOf[Int])
        .dest(Command.KEY_DIM)
        .metavar("DIM")
        .help("dimension of the cluster centers")
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
    if (args.length != 7) {
      throw new RuntimeException("Arguments count !- 7")
    }

    val master: String = args(0)
    val dop: Int = args(0).toInt
    val N: Int = args(0).toInt
    val output: String = args(0)
    val input: String = args(0)
    val K: Int = args(0).toInt
    val dim: Int = args(0).toInt

    val generator = new ClusterGenerator(master, dop, N, output, input, K, dim)
    generator.run()
  }
}

class ClusterGenerator(master: String, dop: Int, N: Int, output: String, input: String, K: Int, dim: Int) extends Algorithm(master) {

  import eu.stratosphere.peel.datagen.spark.ClusterGenerator.Schema.Point

  def this(ns: Namespace) = this(
    ns.get[String](Algorithm.Command.KEY_MASTER),
    ns.get[Int](ClusterGenerator.Command.KEY_DOP),
    ns.get[Int](ClusterGenerator.Command.KEY_N),
    ns.get[String](ClusterGenerator.Command.KEY_OUTPUT),
    ns.get[String](ClusterGenerator.Command.KEY_INPUT),
    ns.get[Int](ClusterGenerator.Command.KEY_K),
    ns.get[Int](ClusterGenerator.Command.KEY_DIM))

  def run() = {
    val conf = new SparkConf().setAppName(new ClusterGenerator.Command().name).setMaster(master)
    val sc = new SparkContext(conf)

    val n = N / dop - 1 // number of points generated in each partition
    val ppc = N / K // number of points per center
    val tDim = this.dim
    val seed = this.SEED

    val csv = sc.textFile(input).map { line =>
      line.split(",").map(_.toDouble)
    }.collect()

    val centroids = sc.broadcast(csv)

    val dataset = sc.parallelize(0 until dop, dop).flatMap(i => {
      val partitionStart = n * i // the index of the first point in the current partition
      val randStart = partitionStart * tDim // the start for the prng
      val rand = new RanHash(seed)
      rand.skipTo(seed + randStart)

      for (j <- partitionStart to (partitionStart + n)) yield {
        val centroidID = j / ppc
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

