import Main.system
import Main.system.dispatcher
import Utils.Conversions.{toNameBasic, toPrincipal, toTitleBasic, toTitleEpisode, toTitlePrincipal, toTvSerie}
import Utils.Operators.tsvSourceMapper
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.GraphDSL.Implicits.port2flow
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Keep, Merge, Sink, Source}

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

  trait MovieService {
    def principalsForMovieName(movieName: String): Source[Principal, _]

    def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _]

  }

  //From an upstream of title principals ids down stream name basic ids
  private val titlePrincipals: Flow[String, String, NotUsed] = {
    Flow[String].flatMapConcat(tConst => titlePrincipalSource
      .filter(tp => tp.tconst == tConst)
      .map(tp => tp.nconst))
  }

  // From an upstream of original movie name downstream title ids
  private val titleByOriginalName: Flow[String, String, NotUsed] = {
    Flow[String].flatMapConcat(originalName => titleBasicSource
      .filter(tb => tb.originalTitle.contains(originalName))
      .map(tb => tb.tconst))
  }

  //For an upstream of name basic ids look up for principals
  private val principals: Flow[String, Principal, NotUsed] = {
    Flow[String].flatMapConcat(nConst => nameBasicSource
      .filter(nb => nb.nconst == nConst)
      .map(toPrincipal))
  }

  //  Filter the Titles in order to pick series only
  private val tvSeriesFlow: Flow[TitleBasic, TitleBasic, NotUsed] =
    Flow[TitleBasic].filter(tb => tb.titleType.contains("tvSeries"))


  // From an upstream  of series count all the episodes of a given title
  // downstream a pair of (number of episodes, title)
  // representing the number of episodes for each title
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
        .log(name = "Counting episodes for each title")
        .addAttributes(Attributes.logLevels(onElement = Attributes.LogLevels.Info, onFinish = Attributes.LogLevels.Info, onFailure = Attributes.LogLevels.Error))

    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val portsNumber = 10
      val titleBalancer = builder.add(Balance[TitleBasic](portsNumber))
      val mergePrincipals = builder.add(Merge[(Int, TitleBasic)](portsNumber))
      for (i <- 0 until portsNumber) {
        titleBalancer.out(i) ~> Flow[TitleBasic].flatMapConcat(tb => titleEpisodesSource.via(titleEpisodeCountingFlow(tb))).async ~> mergePrincipals.in(i)
      }

      FlowShape(titleBalancer.in, mergePrincipals.out)
    })
  }

  private val sortedTvSeries: Sink[(Int, TitleBasic), Future[List[TvSerie]]] = Flow[(Int, TitleBasic)]
    .fold(List.empty[(Int, TitleBasic)])((acc, elem) =>
      if (acc.length < 10)
        elem :: acc.sortWith((l, r) => l._1 > r._1).take(10)
      else
        acc)

    .toMat(Sink.last[List[(Int, TitleBasic)]])(Keep.right)
    .mapMaterializedValue(fs => fs.map(seq => seq
      .map(tb => toTvSerie(tb._2, tb._1))))


  object MovieServiceImpl1 extends MovieService {


    override def principalsForMovieName(movieName: String): Source[Principal, _] = {
      Source.single(movieName)
        .via(titleByOriginalName)
        .async
        .via(titlePrincipals)
        .async
        .via(principals)
    }

    override def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _] = Source.future(
      titleBasicSource
        .via(tvSeriesFlow)
        .async
        .via(countAndFold)
        .runWith(sortedTvSeries))
      .mapConcat(seq => seq)
  }

  object MovieServiceImpl2 extends MovieService {

    // USING PIPELINING
    private val principalsForMovieGraph: Flow[String, Principal, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val portNumber = 10
      val dispatch = builder.add(Balance[String](portNumber))
      val mergePrincipals = builder.add(Merge[Principal](portNumber))
      for (i <- 0 until portNumber) {
        dispatch.out(i) ~> titleByOriginalName.async ~> titlePrincipals.async ~> principals.async ~> mergePrincipals.in(i)
      }
      FlowShape(dispatch.in, mergePrincipals.out)
    })

    private val seriesWithGreatestNumberOfEpisodes: Flow[TitleBasic, (Int, TitleBasic), NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
      val portNumber = 10
      val dispatch = builder.add(Balance[TitleBasic](portNumber))
      val mergePrincipals = builder.add(Merge[(Int, TitleBasic)](portNumber))

      for (i <- 0 until portNumber) {
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
          .via(seriesWithGreatestNumberOfEpisodes)
          .runWith(sortedTvSeries))
        .mapConcat(s => s)
    }
  }
}







