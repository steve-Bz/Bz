
import Main.system.dispatcher
import MovieService.{MovieServiceImpl1, MovieServiceImpl2, titleBasicSource, tvSeriesSortingSink}
import MovieService.MovieServiceImpl1.tvSeriesWithGreatestNumberOfEpisodes
import akka.actor.ActorSystem


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("MediaHub")

   def runPrincipalsForMovieName(): Unit = {

    //Testing Some MovieNames
    //Some movie names
    //Surviving the Social
    //Blood Money	Blood Money
    val movieName ="Blacksmith Scene"

    MovieServiceImpl1.principalsForMovieName(movieName)
      .runForeach(println)
      .onComplete(_ => system.terminate())
  }

   private def runTvSeriesWithGreatestNumberOfEpisodes(): Unit = {
    MovieServiceImpl2.tvSeriesWithGreatestNumberOfEpisodes()
      .runForeach(println)
      .onComplete(_ => system.terminate())
  }

   runPrincipalsForMovieName();
   //runTvSeriesWithGreatestNumberOfEpisodes()

}