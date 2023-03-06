import Main.system
import Main.system.dispatcher
import MovieService.tvSeriesMapper
import Utils.Conversions.{toNameBasic, toPrincipal, toTitleBasic, toTitleEpisode, toTitlePrincipal, toTvSerie}
import Utils.Operators.tsvSourceMapper
import akka.NotUsed
import akka.event.LogMarker
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits.port2flow
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, Merge, Sink, Source}

import scala.concurrent.Future


object MovieService {
  private val titleBasicFile = "files/title.basics.tsv"
  private val titlePrincipalFile = "files/title.principals.tsv"
  private val nameBasicFile = "files/name.basics.tsv"
  private val titleEpisodeFile = "files/title.episode.tsv"


  final case class Principal(name: String, birthYear: Int, deathYear: Option[Int] = None, profession: List[String] = Nil)

  final case class TvSerie(original: String, startYear: Int, endYear: Option[Int], genres: List[String], numberEpisode: Int = 0)

  final case class TitleBasic(tconst: String, titleType: Option[String] = None, primaryTitle: Option[String] = None, originalTitle: Option[String] = None, startYear: Option[Int] = None, endYear: Option[Int] = None, genres: List[String] = Nil)

  final case class TitlePrincipal(tconst: String, nconst: String)

  final case class TitleEpisode(tconst: String, parentTconst: String, seasonNumber: Int = 0, episodeNumber: Int = 0)

  final case class NameBasic(nconst: String, primaryName: Option[String] = None, birthYear: Option[Int] = None, deathYear: Option[Int] = None, primaryProfession: List[String] = Nil)

  private val titleBasicSource: Source[TitleBasic, Future[IOResult]] = tsvSourceMapper(titleBasicFile, toTitleBasic)
  private val titlePrincipalSource: Source[TitlePrincipal, Future[IOResult]] = tsvSourceMapper(titlePrincipalFile, toTitlePrincipal)
  private val nameBasicSource: Source[NameBasic, Future[IOResult]] = tsvSourceMapper(nameBasicFile, toNameBasic)
  private val titleEpisodesSource: Source[TitleEpisode, Future[IOResult]] = tsvSourceMapper(titleEpisodeFile, toTitleEpisode)

  trait MovieService {
    def principalsForMovieName(movieName: String): Source[Principal, _]

    def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _]

  }

  /**
   * Flow that looks up for names ids for its upstream and put them on downstream
   */
  private val lookUpForTitlePrincipal: Flow[String, String, NotUsed] = Flow[String]
    .flatMapConcat(tconst => titlePrincipalSource.via(Flow[TitlePrincipal]
      .filter(tp => tp.tconst == tconst)
      .map(tp => tp.nconst)))


  /**
   * Flow that looks up for Name basis and map them to principal ids for its upstream Lookup for names ids
   */
  private val lookUpForPrincipals: Flow[String, Principal, NotUsed] = Flow[String]
    .flatMapConcat(nConst =>
      nameBasicSource
        .via(Flow[NameBasic]
          .filter(nb => nb.nconst == nConst).map(toPrincipal)))


  /**
   * Filter the Titles in order to pick series only
   */
  private val serieFlow: Flow[TitleBasic, TitleBasic, NotUsed] = Flow[TitleBasic]
    .filter(tb => tb.titleType.contains("tvSeries"))

  /**
   * Looks for all the titles with a given title original name
   *
   * @param originalName
   * @return
   */
  private def lookUpForTitleId(originalName: String): Source[String, Future[IOResult]] = titleBasicSource
    .via(Flow[TitleBasic]
      .filter(tb => tb.originalTitle.contains(originalName))
      .map(tb => tb.tconst))


  /*
     A Flow shap that that multiplex the search of principal across multiple element
  Testing composing  streams
   */
  private val principalsForMovieGraph: Flow[String, Principal, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    val dispatch = builder.add(Balance[String](10))
    val mergePrincipals = builder.add(Merge[Principal](10))
    for (i <- 0 to 9) {
      dispatch.out(i) ~> lookUpForTitlePrincipal.async ~> lookUpForPrincipals.async ~> mergePrincipals.in(i)
    }
    FlowShape(dispatch.in, mergePrincipals.out)
  })

  /**
   * Flow that can be attached to any Source if one want to count all the episodes of the titles in stream
   */
  private val countingEpisodes: Flow[TitleBasic, (Int, TitleBasic), NotUsed] = Flow[TitleBasic]
    .flatMapConcat(titleBasicFile =>
      episodesCounter(titleBasicFile)
        .groupBy(10, tb => tb._2.tconst)
        .mergeSubstreams)


  /**
   * Count episodes for anny given serie
   * Use a stateful map to propagate the accumulator
   * One Count done the total number is folded
   * The downStream will have a flow of TitleBasics with the total of its epidodes
   *
   * @param titleBasic
   * @return
   */
  private def episodesCounter(titleBasic: TitleBasic): Source[(Int, TitleBasic), Future[IOResult]] =
    titleEpisodesSource.via(Flow[TitleEpisode]
      .filter(te => te.parentTconst == titleBasic.tconst)
      .statefulMap(() => (0, titleBasic))((counter, elem) => ((counter._1 + 1, counter._2), (elem, counter)), _ => None)
      .map(tbC => tbC._2))
      .fold(Option.empty[(Int, TitleBasic)])((acc, elem) => acc match {
        case Some((n: Int, _: TitleBasic)) => Some((Integer.max(n, elem._1), elem._2))
        case None => Some(elem._1, elem._2)
      })
      .filter(tb => tb.isDefined)
      .map(tp => tp.get)
      .logWithMarker("Counting Episodes", e => LogMarker(name = "Finished Counting", properties = Map("element" -> e)))
      .addAttributes(
        Attributes.logLevels(
          onElement = Attributes.LogLevels.Info,
          onFinish = Attributes.LogLevels.Info,
          onFailure = Attributes.LogLevels.Error))


  /*
  This Flow mapper all instance of ti
   */
  private val tvSeriesMapper: Flow[(Int, TitleBasic), TvSerie, NotUsed] = Flow[(Int, TitleBasic)].map(ts => toTvSerie(ts._2, ts._1))

  def sortingSink: Sink[TvSerie, Future[Seq[TvSerie]]] = Flow[TvSerie]
    .toMat(Sink.seq[TvSerie])(Keep.right)
    .mapMaterializedValue(fs => fs
      .map(seq => seq
        .sortWith((a, b) => a.numberEpisode < b.numberEpisode)
        .take(10)))

  /*
  Count and materialize the
   */
  val seriesWithGreatestNumberOfEpisodesSink: Sink[TitleBasic, Future[Seq[TvSerie]]] = Flow[TitleBasic]
    .via(countingEpisodes)
    .via(tvSeriesMapper)
    .toMat(sortingSink)(Keep.right)

  object MovieServiceImplB extends MovieService {
    override def principalsForMovieName(movieName: String): Source[Principal, _] = lookUpForTitleId(movieName)
      .via(principalsForMovieGraph)
      .logWithMarker("Principals for movie name", e => LogMarker(name = "Principals for movie name", properties = Map("element" -> e)))
      .addAttributes(
        Attributes.logLevels(
          onElement = Attributes.LogLevels.Info,
          onFinish = Attributes.LogLevels.Info,
          onFailure = Attributes.LogLevels.Error))

    override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _] = {
      Source.future(titleBasicSource // From titles
        .via(serieFlow) // Filter to keep series only
        .toMat(seriesWithGreatestNumberOfEpisodesSink)(Keep.right).run()) // Materialized the flow
        .flatMapConcat(s => Source(s))


    }
  }

  object MovieServiceImplA extends MovieService {
    override def principalsForMovieName(movieName: String): Source[Principal, _] =
      lookUpForTitleId(movieName)  //look up for titles -
        .via(lookUpForTitlePrincipal) //Lo
        .via(lookUpForPrincipals)

    override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, Future[IOResult]] = {
      titleBasicSource
        .via(serieFlow)
        .via(countingEpisodes)
        .via(tvSeriesMapper)
    }
  }
}







