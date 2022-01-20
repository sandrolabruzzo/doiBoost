package eu.dnetlib.dhp.doiboost.uw

import com.fasterxml.jackson.databind.ObjectMapper
import eu.dnetlib.dhp.schema.oaf.OpenAccessRoute
import eu.dnetlib.doiboost.uw.UnpayWallToOAF
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

class UnpayWallMappingTest {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val mapper = new ObjectMapper()

  @Test
  def testMappingToOAF(): Unit = {

    val Ilist = Source
      .fromInputStream(getClass.getResourceAsStream("/eu/dnetlib/doiboost/uw/input.json"))
      .mkString

    var i: Int = 0
    for (line <- Ilist.lines) {
      val p = UnpayWallToOAF.convertToOAF(line)

      if (p != null) {
        assertTrue(p.getInstance().size() == 1)
        if (i == 0) {
          assertTrue(p.getPid.get(0).getValue.equals("10.1038/2211089b0"))
        }
        if (i == 1) {
          assertTrue(p.getPid.get(0).getValue.equals("10.1021/acs.bioconjchem.8b00058.s001"))
        }
        if (i == 2) {
          assertTrue(p.getPid.get(0).getValue.equals("10.1021/acs.bioconjchem.8b00086.s001"))
        }
        logger.info(s"ID : ${p.getId}")
      }
      assertNotNull(line)
      assertTrue(line.nonEmpty)
      i = i + 1
    }

    val l = Ilist.lines.next()

    val item = UnpayWallToOAF.convertToOAF(l)

    assertEquals(
      item.getInstance().get(0).getAccessright.getOpenAccessRoute,
      OpenAccessRoute.bronze
    )

    logger.info(mapper.writeValueAsString(item))

  }

}
