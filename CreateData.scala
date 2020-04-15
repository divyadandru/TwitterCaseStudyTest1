package twitter

import java.io.IOException

import org.json.simple.JSONObject
import org.json.simple.parser.{JSONParser, ParseException}

object CreateData {
  @throws[IOException]
  @throws[ParseException]
  def data(line: String): String = {
    val parser = new JSONParser
    val obj = parser.parse(line)
    val jo = obj.asInstanceOf[JSONObject]

    var s = ""
    val jo1 = jo.get("retweeted_status").asInstanceOf[JSONObject]
    val jo2 = jo.get("extended_tweet").asInstanceOf[JSONObject]

    if (jo1 == null && jo2 == null) s = jo.get("text").asInstanceOf[String]
    else if (jo2 != null) s = jo2.get("full_text").asInstanceOf[String]
    else {
      val jo3 = jo1.get("extended_tweet").asInstanceOf[JSONObject]
      if (jo3 == null) s = jo1.get("text").asInstanceOf[String]
      else s = jo3.get("full_text").asInstanceOf[String]
    }

    val jo4 = jo.get("user").asInstanceOf[JSONObject]
    var location = ""
    if (jo4!=null) location = jo4.get("location").asInstanceOf[String]
    if (location == null || location.equals("")) return "-1"

    val time = jo.get("created_at").asInstanceOf[String]
    val temp = time.split(" ")

    //s = s.filter( _ >= ' ')
    val result = '"' + temp(1) + " " + temp(2) + "," + " " + temp(5) + '"' + "," + '"' + s + '"' + "," + '"' + location + '"'

    result

  }
}

//import java.io.IOException
//
//import org.json.simple.JSONObject
//import org.json.simple.parser.{JSONParser, ParseException}
//
//object CreateData {
//  @throws[IOException]
//  @throws[ParseException]
//  def data(line: String): String = {
//    val parser = new JSONParser
//    val obj = parser.parse(line)
//    val jo = obj.asInstanceOf[JSONObject]
//    var s = ""
//    //    val language = jo.get("lang").asInstanceOf[String]
//    val jo1 = jo.get("retweeted_status").asInstanceOf[JSONObject]
//    val jo2 = jo.get("extended_tweet").asInstanceOf[JSONObject]
//    if (jo1 == null && jo2 == null) s = jo.get("text").asInstanceOf[String]
//    else if (jo2 != null) s = jo2.get("full_text").asInstanceOf[String]
//    else {
//      val jo3 = jo1.get("extended_tweet").asInstanceOf[JSONObject]
//      if (jo3 == null) s = jo1.get("text").asInstanceOf[String]
//      else s = jo3.get("full_text").asInstanceOf[String]
//    }
//    val jo4 = jo.get("user").asInstanceOf[JSONObject]
//    var location = jo4.get("location").asInstanceOf[String]
//    if (location == null) return "-1"
//    val time = jo.get("created_at").asInstanceOf[String]
//    val temp = time.split(" ")
//    //s = s.filter( _ >= ' ')
//    val result = '"' + temp(1) + " " + temp(2) + "," + " " + temp(5) + '"' + "," + '"' + s + '"' + "," + '"' + location + '"'
//    result
//
//  }
//}


