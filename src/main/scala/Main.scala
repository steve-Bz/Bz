
import Main.system.dispatcher
import MovieService.{MovieServiceImplA, MovieServiceImplB, sortingSink}
import MovieService.MovieServiceImplA.tvSeriesWithGreatestNumberOfEpisodes
import akka.actor.ActorSystem


object Main extends App {

  implicit val system: ActorSystem = ActorSystem("MediaHub")

  def runPrincipalsForMovieName(): Unit = {

    //Testing value
    //Some movie names
    //Surviving the Social
    //Blood Money	Blood Money

    //Implementation A
    MovieServiceImplA.principalsForMovieName("Blacksmith Scene")

    //Implement B
    //MovieServiceImplB.principalsForMovieName("Blacksmith Scene")
      .runForeach(println)
      .onComplete(_ => system.terminate())
  }

  private def runTvSeriesWithGreatestNumberOfEpisodes(): Unit = {
    //MovieServiceImplA.tvSeriesWithGreatestNumberOfEpisodes()
    MovieServiceImplB.tvSeriesWithGreatestNumberOfEpisodes()
      .runForeach(println)
      .onComplete(_ => system.terminate())
}


 runPrincipalsForMovieName();
//runTvSeriesWithGreatestNumberOfEpisodes()

}