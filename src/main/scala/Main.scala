
import MovieService.MovieServiceImpl
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

        MovieServiceImpl.principalsForMovieName(message)
          .runForeach(println)
    }
  }

  private class SeriesActor extends Actor {

    override def receive: Receive = {
      case _: String =>
        println(
          """|Hang tight  :) :) :)
             |""".stripMargin)
        MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes().runForeach(println)
    }
  }

  private def run(): Unit = {
    println(
      """
        |Before runing this test make sure  this are in your main/resources directory:
        |title.basics.tsv
        |title.principals.tsv
        |name.basics.tsv
        |title.episode.tsv
        |Are present in your resource file (main/resources) or specify they paths in class MovieService :)
        |
        |""".stripMargin)

    println(
      """|->>  Tape: "principals" or "series"
         |>>>
         |""".stripMargin)

    while (true) {

      val message = scala.io.StdIn.readLine()

      message match {
        case "series" =>
          println("Looking up for series... ")
          seriesActor ! "do it"


        case "principals" =>
          println("""Movie Name:  (hint: "Blacksmith Scene") """)
          val movieName = scala.io.StdIn.readLine()
          principalsActor ! movieName
        case _ => run()
      }
    }
  }

  run()
}