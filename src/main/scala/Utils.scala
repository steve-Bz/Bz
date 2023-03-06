import akka.NotUsed
import akka.stream.IOResult
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Flow, Source}
import akka.util.ByteString

import java.nio.file.Paths
import scala.collection.Map
import scala.concurrent.Future


object Utils {

  object Conversions {
    import MovieService._

    import scala.language.implicitConversions

    implicit def toOptionInt(option: Option[String]): Option[Int] = option.flatMap(s => s.toIntOption)

    implicit def split(option: Option[String], regex: String = ","): List[String] =
      option
        .map(s => s.replaceAll("\"", ""))
        .map(s => s.split(regex)
          .filter(_.nonEmpty).toList)
        .getOrElse(List[String]())

    def toNameBasic(nameBasic: Map[String, String]): Option[NameBasic] = {
      if (nameBasic.isEmpty | !nameBasic.contains("nconst")) None
      else (nameBasic.get("nconst"),
        nameBasic.get("primaryName"),
        nameBasic.get("birthYear"),
        nameBasic.get("deathYear"),
        nameBasic.get("primaryProfession"))
      match {
        case (nconst, primaryName, birthYear, deathYear, primaryProfession) =>
          Some(NameBasic(
            nconst.get,
            primaryName,
            toOptionInt(birthYear),
            toOptionInt(deathYear),
            split(primaryProfession)))
      }
    }

    def toTitleEpisode(titleEpisode: Map[String, String]): Option[TitleEpisode] = {
      if (titleEpisode.isEmpty | (!titleEpisode.contains("tconst") | (!titleEpisode.contains("parentTconst")))) None
      else (titleEpisode.get("tconst"),
        titleEpisode.get("parentTconst"),
        titleEpisode.get("seasonNumber"),
        titleEpisode.get("episodeNumber"))
      match {
        case (tconst, parentTconst, seasonNumber, episodeNumber) =>
          Some(TitleEpisode(
            tconst.get,
            parentTconst.get,
            toOptionInt(seasonNumber).getOrElse(0),
            toOptionInt(episodeNumber).getOrElse(0)))
      }
    }

    def toTitleBasic(titleBasic: Map[String, String]): Option[TitleBasic] = {
      if (titleBasic.isEmpty | (!titleBasic.contains("tconst"))) None
      else (titleBasic.get("tconst"),
        titleBasic.get("titleType"),
        titleBasic.get("primaryTitle"),
        titleBasic.get("originalTitle"),
        titleBasic.get("startYear"),
        titleBasic.get("endYear"),
        titleBasic.get("genres")
      )
      match {
        case (tconst, titleType, primaryTitle, originalTitle, startYear, endYear, genres) =>
          Some(TitleBasic(
            tconst.get,
            titleType,
            primaryTitle,
            originalTitle,
            toOptionInt(startYear),
            toOptionInt(endYear),
            split(genres))
          )
      }
    }

    def toTitlePrincipal(titlePrincipal: Map[String, String]): Option[TitlePrincipal] = {
      if (titlePrincipal.isEmpty) None
      else
        (titlePrincipal.get("tconst"), titlePrincipal.get("nconst")) match {
          case (t, n) => if (t.isEmpty | n.isEmpty) None else Some(TitlePrincipal(t.get, n.get))

        }
    }

    def toPrincipal(nameBasic: NameBasic): Principal = Principal(nameBasic.primaryName.getOrElse("Not Specified"), nameBasic.birthYear.getOrElse(0), nameBasic.deathYear, nameBasic.primaryProfession)

    def toTvSerie(titleBasic: TitleBasic,numberEpisodes: Int): TvSerie = TvSerie(titleBasic.originalTitle.getOrElse("Not Specified"), titleBasic.startYear.getOrElse(0), titleBasic.endYear, titleBasic.genres,numberEpisodes)
  }


  object Operators {

    private val lineParser: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner(delimiter = CsvParsing.Tab, escapeChar = CsvParsing.DoubleQuote)
    private val csvToMap: Flow[List[ByteString], Map[String, String], NotUsed] = CsvToMap.toMapAsStrings()

    def tsvSource(fileName: String): Source[Map[String, String], Future[IOResult]] =
      FileIO.fromPath(Paths.get(fileName))
        .via(lineParser)
        .via(csvToMap)

    def tsvSourceMapper[T](fileName: String, mapper: Map[String, String] => Option[T]): Source[T, Future[IOResult]] =

      tsvSource( getClass.getResource(fileName).getFile)
        .map(mapper)
        .filter(_.nonEmpty)
        .map(_.get)

  }
}
