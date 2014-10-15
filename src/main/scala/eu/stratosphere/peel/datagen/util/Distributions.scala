package eu.stratosphere.peel.datagen.util

object Distributions {

  trait Distribution {
    def sample(rand: RanHash): Double
  }

  case class Gaussian(mu: Double, sigma: Double) extends Distribution {
    def sample(rand: RanHash) = {
      sigma * rand.nextGaussian()
    }
  }

  case class Uniform(k: Int) extends Distribution {
    def sample(rand: RanHash) = {
      rand.nextInt(k)
    }
  }

  case class Pareto(a: Double) extends Distribution {
    def sample(rand: RanHash) = {
      rand.nextPareto(a)
    }
  }
}
