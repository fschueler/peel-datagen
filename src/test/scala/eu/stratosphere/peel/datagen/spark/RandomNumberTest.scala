package eu.stratosphere.peel.datagen.spark

import java.io._

import eu.stratosphere.peel.datagen.util.RanHash
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class RandomNumberTest extends AssertionsForJUnit {

  @Test def NormalTest(): Unit = {
    val output = "data/randGaussian.txt"
    val SEED = 5431423142056L
    val rnd = new RanHash(SEED)
    val N = 1000
    val numbers = for (i <- 1 to N) yield rnd.nextGaussian()

    printToFile(new File(output)) { p =>
      numbers.foreach(p.println)
    }

  }

  @Test def ParetoTest(): Unit = {
    val output = "data/randPareto.txt"
    val SEED = 5431423142056L
    val rnd = new RanHash(SEED)
    val N = 10000
    val alpha = 1
    val numbers = for (i <- 1 to N) yield rnd.nextPareto(alpha)

    printToFile(new File(output)) { p =>
      numbers.foreach(p.println)
    }

  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }



}
