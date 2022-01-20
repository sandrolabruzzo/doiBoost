package eu.dnetlib.dhp.doiboost

import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.junit.jupiter.api.Test

class NormalizeDOITest {

  @Test
  def doiDSLowerCase(): Unit = {
    val doi = "10.1042/BCJ20160876"

    assert(DoiBoostMappingUtil.normalizeDoi(doi).equals(doi.toLowerCase()))

  }

  @Test
  def doiFiltered(): Unit = {
    val doi = "0.1042/BCJ20160876"

    assert(DoiBoostMappingUtil.normalizeDoi(doi) == null)
  }

  @Test
  def doiFiltered2(): Unit = {
    val doi = "https://doi.org/0.1042/BCJ20160876"

    assert(DoiBoostMappingUtil.normalizeDoi(doi) == null)
  }

  @Test
  def doiCleaned(): Unit = {
    val doi = "https://doi.org/10.1042/BCJ20160876"

    assert(DoiBoostMappingUtil.normalizeDoi(doi).equals("10.1042/BCJ20160876".toLowerCase()))
  }

  @Test
  def doiCleaned1(): Unit = {
    val doi = "https://doi.org/10.1042/ BCJ20160876"

    assert(DoiBoostMappingUtil.normalizeDoi(doi).equals("10.1042/BCJ20160876".toLowerCase()))
  }

}
