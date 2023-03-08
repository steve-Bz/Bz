
import Main.system.dispatcher
import MovieService.{MovieServiceImpl1, MovieServiceImpl2}
import akka.actor.{Actor, ActorSystem, Props}


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("MediaHub")
  private val principalsActor = system.actorOf(Props[PrincipalsActor], "PrincipalsActor")
  private val seriesActor = system.actorOf(Props[SeriesActor], "SeriesActor")

  private class PrincipalsActor extends Actor {

    override def receive: Receive = {
      case message: String =>
        println(
          s"""All principals in: $message
             |Hang on :) :)
             |""".stripMargin)
        MovieServiceImpl2.principalsForMovieName(message)
          .runForeach(println)
          .onComplete({
            print("Tada !! Here are the principals i found ")
            _ => system.terminate()
          })
    }
  }

  private class SeriesActor extends Actor {
    override def receive: Receive = {
      case _: String =>
        println(
          """|Hang tight this make take some time depends on data set but dont worry I log every thing i do to keep you awake :) :) :)
            |""".stripMargin)
        MovieServiceImpl1.tvSeriesWithGreatestNumberOfEpisodes().runForeach(println).onComplete(_ => {
          print("Tada !! Here are the TV series i found ")
          system.terminate()
        })
    }
  }


  def runner(): Unit = {

    println(
      """|->>  Tape: "principals" / "series"
         |>>>
         |""".stripMargin)

    while (true) {

      val message = scala.io.StdIn.readLine()

      message match {
        case "series" =>
          seriesActor ! "do it"
          println(" Ok lets do it")
        case "principals" =>
          println("""Movie Name:  (hint: you cane tape  "Blacksmith Scene" for example) """)
          val movieName = scala.io.StdIn.readLine()
          principalsActor ! movieName
        case _ => runner()
      }

    }
  }
  runner()
}