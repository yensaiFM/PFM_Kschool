import java.util.concurrent.Callable

import batch._
import utils.Utils.{getSQLContext, getSparkContext}

object Manager {
  def main(args: Array[String]) : Unit = {
    val sc = getSparkContext("PMP Kschool Spark")

    val Batch = new BatchJob(sc)
    Batch.start()

    while(true){
      if (Batch.isAlive() == false) {
        Batch.process
        Batch.addIteration()
        //println("Iniciamos Batch")
      }
      if (Batch.getIteration() == 2) {
        Batch.truncateStreamViews()
        Batch.resetIteration()
      }
    }
  }
}
