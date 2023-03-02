import MovieService.{Principal, TvSerie}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{FileIO, Source}

import java.nio.file.Paths

object Utils {

  implicit class MapString(val map: Map[String, String]) {
    def allMatch(pairs: (String, String)*): Boolean = pairs.forall(p => map.get(p._1).contains(p._2))

    def getStrings(key: String): List[String] = map.get(key).map(_.split(",").toList).orNull

    def getOptionalInt(key: String): Option[Int] = map.get(key).flatMap(s => if (s.nonEmpty & s != """""\N""" & s.forall(_.isDigit)) Option.apply(s.toInt) else Option.empty[Int])
  }

  def principalFromMap(fields: Map[String, String]): Principal = {
    Principal(
      fields("primaryName"),
      fields.getOptionalInt("birthYear").getOrElse(0),
      fields.getOptionalInt("deathYear"),
      fields.getStrings("primaryProfession"))
  }

  def tvSerieFromMap(fields: Map[String, String]): TvSerie = {
    TvSerie(
      fields("originalTitle"),
      fields.getOptionalInt("startYear").getOrElse(0),
      fields.getOptionalInt("endYear"),
      fields.getStrings("genres")
    )
  }

  def tsvSource(fileName: String): Source[Map[String, String], _] = FileIO
    .fromPath(Paths.get(fileName))
    .via(CsvParsing.lineScanner(delimiter = CsvParsing.Tab, escapeChar = CsvParsing.DoubleQuote).log("CSV PARSING ERROR"))
    .via(CsvToMap.toMapAsStrings().log("Csv ressource"))

}
