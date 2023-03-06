import MovieService.{NameBasic, TitleBasic, TitleEpisode, TitlePrincipal}
import Utils.Conversions.{toNameBasic, toTitleBasic, toTitleEpisode, toTitlePrincipal}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.HashMap

class UtilsTest extends AnyFlatSpec with Matchers {

  // Converters
  "Mapping Title principal " should "retrieve principal" in {
    toTitlePrincipal(HashMap.empty) shouldBe None
    toTitlePrincipal(HashMap(("tconst", "ty"), ("nconst", "ty"))) should contain(TitlePrincipal("ty", "ty"))
  }

  "Mapping names basics " should "retrieve name" in {
    toNameBasic(HashMap.empty) shouldBe None
    toNameBasic(HashMap(("nconst", "ty"))) should contain(NameBasic("ty", None, None, None, Nil))
    val name = toNameBasic(HashMap(
      ("nconst", "ty"),
      ("birthYear", "98"),
      ("deathYear", """\N"""),
      ("primaryProfession", """author,actor""")))

    name should contain(NameBasic("ty",
      None,
      birthYear = Some(98),
      deathYear = None,
      primaryProfession = List("author", "actor")
    ))

  }

  "Mapping episodes " should "retrieve episodes" in {
    toTitleEpisode(HashMap.empty) shouldBe None
    toTitleEpisode(HashMap(("tconst", "ty"), ("parentTconst", "ty"))) should contain(TitleEpisode("ty", "ty", 0))
    val name = toTitleEpisode(HashMap(
      ("tconst", "ty"),
      ("parentTconst", "ty"),
      ("seasonNumber", "98"),
      ("episodeNumber", "90")))

    name should contain(TitleEpisode("ty", "ty", seasonNumber = 98, episodeNumber = 90,
    ))
  }

  "Mapping titles " should "retrieve titles" in {
    toTitleBasic(HashMap.empty) shouldBe None
    toTitleBasic(HashMap(("tconst", "ty"))) should contain(TitleBasic("ty", None, None, None, None, None))
    val name = toTitleBasic(HashMap(
      ("tconst", "ty"),
      ("titleType", "titleType"),
      ("primaryTitle", "primaryTitle"),
      ("originalTitle", "originalTitle"),
      ("startYear", "98"),
      ("endYear", "90")))

    name should contain(TitleBasic("ty", Some("titleType"), Some("primaryTitle"), Some("originalTitle"), Some(98), Some(90)))

  }

}
