
import Main.system.dispatcher
import MovieService.MovieService
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source



object Main extends App with MovieService  {

  implicit val system: ActorSystem = ActorSystem("MediaHub")

  val movieaName = "Blacksmith Scene"
    principalsForMovieName("Blacksmith Scene")
      .runForeach(println)
      .onComplete(_ => system.terminate())

  override def tvSeriesWithGreatestNumberOfEpisodes(): Source[MovieService.TvSerie, _] = ???
}
˚