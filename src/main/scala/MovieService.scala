
import Utils._
import akka.stream.scaladsl.Source

object MovieService {

  private val titlesFile = "/Users/stevebizimungu/workspace/lab/akka/mediahub/src/main/resources/title.basics.tsv"
  private val principalsFile = "/Users/stevebizimungu/workspace/lab/akka/mediahub/src/main/resources/title.principals.tsv"
  private val namesFile = "/Users/stevebizimungu/workspace/lab/akka/mediahub/src/main/resources/name.basics.tsv"


  final case class Principal(name: String, birthYear: Int, deathYear: Option[Int], profession: List[String])
  final case class TvSerie(original: String, startYear: Int, endYear: Option[Int], genres: List[String])


  private def lookupForTitleConst(tconst: String): Source[String, _] = {
    tsvSource(principalsFile)
      .filter(_.allMatch(("tconst", tconst)))
      .map(m => m("nconst"))
      .log("ERROR LOOKING UP")
  }

  private def lookUpForPrincipals(nConst: String) = {
    tsvSource(namesFile)
      .filter(_.allMatch(("nconst", nConst)))
      .map(principalFromMap)
  }

  trait MovieService {
    def principalsForMovieName(movieaName: String): Source[Principal, _] = {
      tsvSource(titlesFile)
        .filter(_.allMatch(("primaryTitle", movieaName)))
        .map(m => m("tconst"))
        .flatMapConcat(lookupForTitleConst)
        .flatMapConcat(lookUpForPrincipals)
        .log("PRINCIPAL FOR MOVIE ERROR")
    }

    def tvSeriesWithGreatestNumberOfEpisodes(): Source[TvSerie, _]
  }
}







