package twitter

import maps.{mapper,FullCountryName,CountryName2,CountryName3,IndiaCities,USACities,IndiaStates,SpainCities,ItalyCities}
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

import org.mongodb.scala._
import org.mongodb.scala.{Completed, MongoClient, MongoCollection, Observer}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._


object TwitterConsumer {

  def main(args: Array[String]): Unit = {

    consumeFromKafka("Topic2")

  }

  def putInDB(value: String,collection: MongoCollection[Document]): Unit = {

    println("STARTING...")

    val splitString = value.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")


    if (splitString.length == 3) {

      val fullDate = splitString(0)
      val splitDate = fullDate.split(", ")
      val month = mapper(splitDate(0).substring(1,4))
      val date = splitDate(0).substring(5,splitDate(0).length)
      var year = 0
      if(fullDate.length==2){
        year = splitString(1).substring(0,4).toInt}
      else  {
        year = 2020}
      val Date = year + "-" + month + "-" + date


      println("work")
      val fullTweet = splitString(1)
      val tweet = fullTweet.substring(1, fullTweet.length - 1)


      val loc = splitString(2).split(", ")
      var location = loc(loc.length - 1)

      if (location.length == 1)
        location = loc(0)
      var locationLength = location.length

      if (locationLength != 1) {
        if (location(0) == '"') {
          location = location.substring(1, locationLength)
          locationLength -= 1
        }
        if (location(locationLength - 1) == '"') {
          location = location.substring(0, locationLength - 1)
        }
      }

      var found = 0


      if (location.length == 2) {
        val newLocation = location.toUpperCase()
        val FoundCountry = CountryName2.get(newLocation)
        if (FoundCountry.isDefined) {
          found = 1
          location = FoundCountry.get
        }
      }

      if (found == 0 && location.length == 3) {
        val newLocation = location.toUpperCase()
        val FoundCountry = CountryName3.get(newLocation)
        if (FoundCountry.isDefined) {
          found = 1
          location = FoundCountry.get
        }
      }
      if (found == 0) {
        var firstCharacter = location(0).toLower
        val asciiOfCharacter = firstCharacter.toInt
        val firstCharacter1 = firstCharacter.toString
        if (asciiOfCharacter >= 97 && asciiOfCharacter <= 122 && firstCharacter1 != "x") {
          val listOfCountry = FullCountryName(firstCharacter1)
          if (listOfCountry.contains(location.capitalize)) {
            found = 1
            location = location.capitalize
          }
        }
      }

      breakable {
        if (found == 0) {
          while(true){
            if(IndiaCities.contains(location.capitalize)){
              found  =1
              location = "India"
              break()
            }
            if(IndiaStates.contains(location.capitalize)){
              found = 1
              location = "India"
              break()
            }
            if(USACities.contains(location.capitalize)){
              found = 1
              location = "United States of America"
              break()
            }
            if(SpainCities.contains(location.capitalize)){
              found = 1
              location = "Spain"
              break()
            }
            if(ItalyCities.contains(location.capitalize)){
              found = 1
              location = "Italy"
              break()
            }
            break()
          }
        }}

      if (found == 1) {
        val doc: Document = Document("date" -> s"$Date", "location" -> s"$location", "tweet" -> s"$tweet")
        collection.insertOne(doc)
          .subscribe(new Observer[Completed] {
            override def onNext(result: Completed): Unit = println("Inserted")

            override def onError(e: Throwable): Unit = println("Failed")

            override def onComplete(): Unit = println("Completed")
          })

        //              val documents = [{Document("date" -> s"$date"), Document("date" -> s"$date"), Document("date" -> s"$date"), Document("date" -> s"$date")}]
        //              collection.insertMany(documents)

        Thread.sleep(1)
      }
    }
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "Twitter_consumer_group")

    val mongoClient: MongoClient = MongoClient()
    val database: MongoDatabase = mongoClient.getDatabase("FinalDB")
    val collection: MongoCollection[Document] = database.getCollection("Tweets")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        if (data.value() != "-1") {
          println(data.value().toString)
          putInDB(data.value().toString,collection)
        }
      }
    }
  }
}


