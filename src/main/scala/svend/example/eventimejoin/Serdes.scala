package svend.example.eventimejoin

import java.util
import java.io.Closeable

import org.apache.kafka.common.serialization.{Deserializer, Serializer, Serde}
import play.api.libs.json.{JsResult, Json}


trait NoBoilerPlate extends Closeable {
  override def close(): Unit = {}
  def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
}

object MoodRecJsonSerde extends Serde[MoodRec] with NoBoilerPlate {

  implicit val moodRecReads = Json.reads[MoodRec]
  implicit val moodWrites = Json.writes[MoodRec]

  def parseJson(rawJson: String): JsResult[MoodRec] =
    Json.parse(rawJson).validate[MoodRec]

  override def deserializer() = new Deserializer[MoodRec] with NoBoilerPlate {
    override def deserialize(topic: String, data: Array[Byte]): MoodRec =
      parseJson(new String(data, "UTF-8")).get
  }

  override def serializer() = new Serializer[MoodRec] with NoBoilerPlate{
    def serialize(topic: String, moodRec: MoodRec): Array[Byte] =
      Json.toJson(moodRec).toString().getBytes("UTF-8")
  }

}

object MoodListJsonSerde extends Serde[List[Mood]] with NoBoilerPlate {

  implicit val moodReads = Json.reads[Mood]
  implicit val moodWrites = Json.writes[Mood]

  def parseJsonMood(rawJson: String): JsResult[Mood] =
    Json.parse(rawJson).validate[Mood]

  def parseJsonMoodList(rawJson: String): JsResult[List[Mood]] =
    Json.parse(rawJson).validate[List[Mood]]


  override def deserializer() = new Deserializer[List[Mood]] with NoBoilerPlate {
    override def deserialize(topic: String, data: Array[Byte]): List[Mood] =
      parseJsonMoodList(new String(data, "UTF-8")).get
  }

  override def serializer() = new Serializer[List[Mood]] with NoBoilerPlate{
    def serialize(topic: String, moodList: List[Mood]): Array[Byte] =
      Json.toJson(moodList).toString().getBytes("UTF-8")
  }

}


