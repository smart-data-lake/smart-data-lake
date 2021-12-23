package io.smartdatalake.util.webservice

object WebserviceMethod extends Enumeration {
  type WebserviceMethod = Value

  val Get = Value("GET")
  val Post = Value("POST")
  val Put = Value("PUT")
  val Delete = Value("DELETE")
}