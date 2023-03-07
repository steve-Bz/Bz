
import Main.system.dispatcher
import MovieService.{MovieServiceImpl, MovieServiceWithGraphComposition, tvSeriesSortingSink}
import MovieService.MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes
import akka.actor.ActorSystem


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("MediaHub")

  def runPrincipalsForMovieName(): Unit = {

    //Testing Some MovieNames
    //Some movie names
    //Surviving the Social
    //Blood Money	Blood Money

    MovieServiceWithGraphComposition.principalsForMovieName("Blacksmith Scene")
      .runForeach(println)
      .onComplete(_ => system.terminate())
  }

  private def runTvSeriesWithGreatestNumberOfEpisodes(): Unit = {
    MovieServiceImpl.tvSeriesWithGreatestNumberOfEpisodes()
   // MovieServiceWithGraphComposition.tvSeriesWithGreatestNumberOfEpisodes()
      .runForeach(println)
      .onComplete(_ => system.terminate())
  }


  //runPrincipalsForMovieName();
  runTvSeriesWithGreatestNumberOfEpisodes()

}