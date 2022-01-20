package eu.dnetlib.dhp.doiboost

import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.junit.jupiter.api.Test

class DoiBoostHostedByMapTest {

  @Test
  def idDSGeneration(): Unit = {
    val s = "doajarticles::0066-782X"

    println(DoiBoostMappingUtil.generateDSId(s))

  }

}
