package com.api
import com.typesafe.config.Config
import scala.collection.JavaConverters._

class AppConf(config: Config) extends Serializable {

  val dataurl = config.getConfig("url")
  val url = dataurl.getString("link")
  val gpost = dataurl.getString("get1")
  val getP = dataurl.getString("geta")
  val post= dataurl.getString("post")

}