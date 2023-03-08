import Main.system
import Main.system.dispatcher
import MovieService.TitleBasic
import Utils.Conversions.{toNameBasic, toPrincipal, toTitleBasic, toTitleEpisode, toTitlePrincipal, toTvSerie}
import Utils.Operators.tsvSourceMapper
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits.port2flow
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source}
import org.reactivestreams.Publisher

import scala.concurrent.Future


object MovieService {


  private val titleBasicFile = "title.basics.tsv"
  private val titlePrincipalFile = "title.principals.tsv"
  private val nameBasicFile = "name.basics.tsv"
  private val titleEpisodeFile = "title.episode.tsv"


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

  //Config val

  private val inputBufferSize = 64

  private val maxParallelPort = 100

  trait MovieService {
    def principalsForMovieName(movieName: String): Source[Principal, _]

    def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _]

  }

  /**
   * Flow that looks up for names ids for its upstream and put them on downstream
   */
  private val lookUpForTitlePrincipal: Flow[String, TitlePrincipal, NotUsed] = {
    val fileter: String => RunnableGraph[Publisher[TitlePrincipal]] =
      (tConst: String) => titlePrincipalSource
        .via(Flow[TitlePrincipal]
          .filter(tp => tp.tconst == tConst))
        .toMat(Sink.asPublisher[TitlePrincipal](fanout = true))(Keep.right)

    Flow[String].async("", 64)
      .flatMapConcat(s => Source.fromPublisher(fileter(s).run()))
  }


  /**
   * Flow that looks up for Name basis and map them to principal ids for its upstream Lookup for names ids
   */
  private val lookUpForPrincipals: Flow[String, NameBasic, NotUsed] = {
    val filter: String => RunnableGraph[Publisher[NameBasic]] = (nConst: String) =>
      nameBasicSource
        .via(Flow[NameBasic]
          .filter(nb => nb.nconst == nConst))
        .toMat(Sink.asPublisher[NameBasic](fanout = true))(Keep.right)

    Flow[String].async("", 64)
      .flatMapConcat(nConst => Source.fromPublisher(filter(nConst).run()))
  }

  /**
   * Filter the Titles in order to pick series only
   */
  private val tvSeriesFlow: Flow[TitleBasic, TitleBasic, NotUsed] =
    Flow[TitleBasic]
      .filter(tb => tb.titleType.contains("tvSeries"))

  /**
   * Looks for all the titles with a given title original name
   */
  private val lookUpForTitleId: Flow[String, TitleBasic, NotUsed] = {
    val filter = (originalName: String) => titleBasicSource
      .via(Flow[TitleBasic]
        .filter(tb => tb.originalTitle.contains(originalName)))
      .toMat(Sink.asPublisher[TitleBasic](fanout = true))(Keep.right)

    Flow[String].async("", inputBufferSize)
      .flatMapConcat(s => Source.fromPublisher(filter(s).run()))
  }


  private val countAndFold: Flow[TitleBasic, (Int, TitleBasic), NotUsed] = {
    val titleEpisodeCountingFlow: TitleBasic => Flow[TitleEpisode, (Int, TitleBasic), NotUsed] =
      (titleBasic: TitleBasic) => Flow[TitleEpisode]
        .filter(te => te.parentTconst == titleBasic.tconst)
        .statefulMap(() => (0, titleBasic))((counter, elem) => ((counter._1 + 1, counter._2), (elem, counter)), _ => None)
        .map(tbC => tbC._2)
        .fold(Option.empty[(Int, TitleBasic)])((acc, elem) => acc match {
          case Some((n: Int, _: TitleBasic)) => Some((Integer.max(n, elem._1), elem._2))
          case None => Some(elem._1, elem._2)
        })
        .takeWhile(tb => tb.isDefined)
        .map(tp => tp.get)
        .log(name = "Count and fold Titles")
        .addAttributes(
          Attributes.logLevels(
            onElement = Attributes.LogLevels.Info,
            onFinish = Attributes.LogLevels.Info,
            onFailure = Attributes.LogLevels.Error))

    val countingDispatcher: Flow[TitleBasic, (Int, TitleBasic), NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val balanceTitles = builder.add(Balance[TitleBasic](maxParallelPort))

      val mergePrincipals = builder.add(Merge[(Int, TitleBasic)](maxParallelPort))

      for (i <- 0 until maxParallelPort) {
        balanceTitles.out(i) ~> Flow[TitleBasic].flatMapConcat(tb => titleEpisodesSource.via(titleEpisodeCountingFlow(tb))).async ~> mergePrincipals.in(i)
      }
      FlowShape(balanceTitles.in, mergePrincipals.out)
    })
    countingDispatcher
  }

  val tvSeriesSortingSink: Sink[(Int, TitleBasic), Future[Seq[TvSerie]]] = Flow[(Int, TitleBasic)]
    .toMat(Sink.seq[(Int, TitleBasic)])(Keep.right)
    .mapMaterializedValue(fs => fs
      .map(seq => seq
        .sortWith((l, r) => l._1 < r._1)
        .map(tb => toTvSerie(tb._2, tb._1))))


  object MovieServiceWithGraphComposition extends MovieService {

    /*
       A Flow  that that multiplex the search of principal across multiple element
    Testing composing  streams
     */
    private val principalsForMovieGraph: Flow[String, Principal, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val dispatch = builder.add(Balance[String](maxParallelPort))
      val mergePrincipals = builder.add(Merge[Principal](maxParallelPort))
      for (i <- 0 to maxParallelPort) {
        dispatch.out(i) ~> lookUpForTitleId.async ~> Flow[TitleBasic].map(tb => tb.tconst).async ~> lookUpForTitlePrincipal.async ~> Flow[TitlePrincipal].map(tp => tp.nconst).async ~> lookUpForPrincipals.async ~> Flow[NameBasic].map(toPrincipal) ~> mergePrincipals.in(i)
      }
      FlowShape(dispatch.in, mergePrincipals.out)
    })

    private val seriesWithGreatestNumberOfEpisodes: Flow[TitleBasic, (Int, TitleBasic), NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val dispatch = builder.add(Balance[TitleBasic](maxParallelPort))

      val mergePrincipals = builder.add(Merge[(Int, TitleBasic)](maxParallelPort))

      for (i <- 0 until maxParallelPort) {
        dispatch.out(i) ~> tvSeriesFlow.async ~> countAndFold.async ~> mergePrincipals.in(i)
      }

      FlowShape(dispatch.in, mergePrincipals.out)
    })


    override def principalsForMovieName(movieName: String): Source[Principal, _] =
      Source.single(movieName)
        .via(principalsForMovieGraph)


    override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _] = {

      Source.future(
        titleBasicSource
          .async("", inputBufferSize)
          .via(tvSeriesFlow)
          .async("", inputBufferSize)
          .via(countAndFold)
          .runWith(tvSeriesSortingSink))
        .mapConcat(s => s)
    }
  }

  object MovieServiceImpl extends MovieService {


    override def principalsForMovieName(movieName: String): Source[Principal, _] = {
      Source.single(movieName)
        .via(lookUpForTitleId)
        .map(tb => tb.tconst)
        .via(lookUpForTitlePrincipal)
        .async
        .map(tp => tp.nconst)
        .via(lookUpForPrincipals)
        .async
        .map(toPrincipal)
    }

    override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _] = Source.future(
      titleBasicSource
        .via(tvSeriesFlow)
        .via(countAndFold)
        .runWith(tvSeriesSortingSink))
      .mapConcat(seq => seq)
  }
}







