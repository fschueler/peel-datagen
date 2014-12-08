package eu.stratosphere.peel.datagen.spark

import java.io.File

import eu.stratosphere.peel.datagen.spark.SparkTupleGenerator.Distributions
import org.apache.commons.io.FileUtils
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class SparkTupleGeneratorTest extends AssertionsForJUnit {

  @Test def integrationTest() {
    val numTasks = 1
    val tuplesPerTask = 1000000
    val payload = 5
    val keyDist = Distributions.Pareto
    val lower = 1
    val upper = 1000
    // master with given numTasks
    val master = s"local[$numTasks]"
    // input and output path
    val input = getClass.getResource("/clusterCenters.csv")
    val output = s"${System.getProperty("java.io.tmpdir")}/data/tupleGeneratorOutput"

    // delete output file if exists
    FileUtils.deleteDirectory(new File(output))

    val gen =new SparkTupleGenerator(master, numTasks, tuplesPerTask, lower, upper, keyDist, payload, output)
    gen.run()
  }
}
