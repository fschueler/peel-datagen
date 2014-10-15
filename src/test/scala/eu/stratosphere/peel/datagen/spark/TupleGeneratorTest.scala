package eu.stratosphere.peel.datagen.spark

import eu.stratosphere.peel.datagen.util.Distributions.Pareto
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class TupleGeneratorTest extends AssertionsForJUnit {

  @Test def integrationTest() {
    val output = "data/tupleGeneratorOutput"
    val master = "local[3]"
    // N should have a common demnominator with K and dop
    val dop = 3
    val N = 10000
    val pay = 5
    val dist = Pareto(1)

    val gen = new TupleGenerator(master, dop, N, output, dist, pay)
    gen.run()

  }

}
