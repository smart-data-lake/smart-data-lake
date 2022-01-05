package io.smartdatalake.util.json

import com.typesafe.config.{Config, ConfigRenderOptions}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{Formats, JValue}

object JsonUtils {

  /**
   * Convert a Json value to Json String
   */
  def jsonToString(json: JValue)(implicit formats: Formats): String = {
    JsonMethods.compact(json)
  }

  /**
   * Convert a case class to a Json String
   */
  def caseClassToJsonString(instance: AnyRef)(implicit formats: Formats): String = {
    Serialization.write(instance)
  }

  /**
   * Convert a Hocon config to a Json String
   */
  def configToJsonString(config: Config)(implicit formats: Formats): String = {
    config.root().render(ConfigRenderOptions.concise())
  }

}
