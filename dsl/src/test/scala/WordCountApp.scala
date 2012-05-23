import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait WordCountApp extends DListProgram with ApplicationOps with SparkDListOps with StringAndNumberOps {

  def parse(x: Rep[String]): Rep[String] = {
    val splitted = x.split("\\s+")
    splitted.apply(2)
  }

  def wikiArticleWordcount2012(x: Rep[Unit]) = {
    val splitted = stopwords.split("\\s").
      toList.filter(_.length > 1).
      flatMap { x => scala.collection.immutable.List(x, x.capitalize) }
    val stopWordsList = unit(splitted.toArray)
    val stopWordsSet = Set[String]()
    stopWordsList.foreach { x =>
      stopWordsSet.add(x)
    }
    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map(x => "\\n" + x.plaintext)
      .map(_.replaceAll("""\[\[.*?\]\]""", " "))
      //      .flatMap(_.replaceAll("""((\\n((\d+px)?(left|right)?thumb(nail)?)*)|(\.(thumb)+))""", " ")
      //          .split("[^a-zA-Z0-9']+").toSeq)
      //      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      .flatMap(_.replaceAll("""\\[nNt]""", " ").split("[^a-zA-Z0-9']+").toSeq)
      .filter(x => x.length > 1)
      .filter(x => !stopWordsSet.contains(x))
      .map(x => if (x.matches("^((left|right)*(thumb)(nail|left|right)*)+[0-9A-Z].*?")) x.replaceAll("((left|right)*thumb(nail|left|right)*)+", "") else x)
      //      .filter(x => !((x.contains("thumb") || x.contains("0px")) && x.matches("(\\d+px|left|right|thumb|nail)+")))
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .save(getArgs(1))
    //    parsed.map(_.plaintext).save(getArgs(1))
    unit(())
  }

  def wikiArticleWordcount2009(x: Rep[Unit]) = {
    val splitted = stopwords.split("\\s").
      toList.filter(_.length > 1).
      flatMap { x => scala.collection.immutable.List(x, x.capitalize) }
    val stopWordsList = unit(splitted.toArray)
    val stopWordsSet = Set[String]()
    stopWordsList.foreach { x =>
      stopWordsSet.add(x)
    }
    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map(_.plaintext)
      //.map(x => if (x.startsWith("thumb")) x.substring(5) else x)
      .flatMap(_.replaceAll("""((\\n(thumb)*)|(\.(thumb)+))""", " ").split("[^a-zA-Z0-9']+").toSeq)
      //.flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      .map(x => if (x.matches("(thumb)+[A-Z].*?")) x.replaceAll("(thumb)+", "") else x)
      .filter(x => x.length > 1)
      .filter(x => !stopWordsSet.contains(x))
      .filter(x => !x.matches("(left|right)(thumb)+"))
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .save(getArgs(1))
    //    parsed.map(_.plaintext).save(getArgs(1))
    unit(())
  }

  def statistics(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    val parsed = read.map(parse)
      .filter(_.matches(".*?en\\.wiki.*?/wiki/.*"))
    //    .filter(_.contains("/wiki/"))
    val parts = parsed.map(_.split("/+").last)
      .filter(!_.matches("[A-Za-z_]+:(?!_).*"))
    parts
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .filter(_._2 >= 5)
      .save(getArgs(1))
    //    parsed.save(folder+"/output/")
    unit(())
  }

  val stopwords = """a
about
above
after
again
against
all
am
an
and
any
are
aren't
as
at
be
because
been
before
being
below
between
both
but
by
can't
cannot
could
couldn't
did
didn't
do
does
doesn't
doing
don't
down
during
each
few
for
from
further
had
hadn't
has
hasn't
have
haven't
having
he
he'd
he'll
he's
her
here
here's
hers
herself
him
himself
his
how
how's
i
i'd
i'll
i'm
i've
if
in
into
is
isn't
it
it's
its
itself
let's
me
more
most
mustn't
my
myself
no
nor
not
of
off
on
once
only
or
other
ought
our
ours
ourselves
out
over
own
same
shan't
she
she'd
she'll
she's
should
shouldn't
so
some
such
than
that
that's
the
their
theirs
them
themselves
then
there
there's
these
they
they'd
they'll
they're
they've
this
those
through
to
too
under
until
up
very
was
wasn't
we
we'd
we'll
we're
we've
were
weren't
what
what's
when
when's
where
where's
which
while
who
who's
whom
why
why's
with
won't
would
wouldn't
you
you'd
you'll
you're
you've
your
yours
yourself
yourselves
"""

}

class WordCountAppGenerator extends CodeGeneratorTestSuite {

  val appname = "WordCountApp"
  val unoptimizedAppname = appname + "_Orig"

  def testBoth {
    tryCompile {
      println("-- begin")

      val dsl = new WordCountApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp
      val codegenSpark = new SparkGenDList { val IR: dsl.type = dsl }
      val codegenScoobi = new ScoobiGenDList { val IR: dsl.type = dsl }
      val list = List(codegenSpark, codegenScoobi)
      def writeVersion(version: String) {
        var pw = setUpPrintWriter
        codegenSpark.emitProgram(dsl.wikiArticleWordcount2012, appname, pw, version)
        writeToProject(pw, "spark", appname, version, codegenSpark.lastGraph)
        release(pw)
        pw = setUpPrintWriter
        codegenScoobi.emitProgram(dsl.wikiArticleWordcount2012, appname, pw, version)
        writeToProject(pw, "scoobi", appname, version, codegenScoobi.lastGraph)
        release(pw)
      }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
      }
      dsl.disablePatterns = true
      writeVersion("v0")
      
      dsl.disablePatterns = false
      writeVersion("v1")
      
      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      writeVersion("v2")
      println("-- end")
    }
  }

}
