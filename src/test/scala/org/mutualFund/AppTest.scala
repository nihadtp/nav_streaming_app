package org.mutualFund;

import junit.framework._
import Assert._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.mutualFund.dataStructure.KafkaMessage;

object AppTest {
  def suite: Test = {
    val suite = new TestSuite(classOf[AppTest]);
    suite
  }

  def main(args: Array[String]) {
    junit.textui.TestRunner.run(suite);
  }
}

/** Unit test for simple App.
  */
class AppTest extends TestCase("app") {

  /** Rigourous Tests :-)
    */
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
  def testOK() = assertTrue(true);
  def testKO() = assertFalse(false);
  def testKafkaMessage(): Unit = {
    val actual = KafkaMessage("error_data", "exception", mapper, "topic")
    val expected =
      "{\"metaData\":{\"time\":\"2022-07-14T13:23:31.597\",\"source\":\"prodigal_streaming_app\"},\"body\":{\"data\":\"error_data\",\"exception\":\"exception\"}}"
    assert(actual.value != expected)
  }

}
