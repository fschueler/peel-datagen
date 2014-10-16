package eu.stratosphere.peel.datagen.spark

import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class SparkSparkClusterGeneratorTest extends AssertionsForJUnit {

  @Test def integrationTest() {
    val input = getClass.getResource("/clusterCenters.csv")
    val output = "/tmp/data/clusterGeneratorOutput"
    val master = "local[3]"
    // N should have a common demnominator with K and dop
    val dop = 3
    val N = 9999

    val gen = new SparkClusterGenerator(master, dop, N, output, input.toString)
    gen.run()

  }

}
