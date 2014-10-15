package eu.stratosphere.peel.datagen.spark

import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class SparkClusterGeneratorTest extends AssertionsForJUnit {

  @Test def integrationTest() {
    val input = getClass.getResource("/clusterCenters.csv")
    val output = "data/clusterGeneratorOutput"
    val master = "local[3]"
    // N should have a common demnominator with K and dop
    val dop = 3
    val N = 99
    val K = 3
    val dim = 3

    val gen = new ClusterGenerator(master, dop, N, output, input.toString, K, dim)
    gen.run()

  }

}
