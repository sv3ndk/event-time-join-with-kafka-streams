package svend.example.eventimejoin

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.{KeyValueStore, WindowStore}
import play.api.libs.json._

import scala.collection.JavaConverters._

/**
  * timestamped recommendation of a consultant: "let's synergize the out-of-the-box ROI" !
  * */
case class Recommendation(event_time: Long, ingestion_time: Long, consultant: String, recommendation: String)

object Recommendation {

  implicit val recommendationReads = Json.reads[Recommendation]
  implicit val recommendationWrites = Json.writes[Recommendation]

  def parseJson(rawJson: String): JsResult[Recommendation] =
    Json.parse(rawJson).validate[Recommendation]
}

/**
  * timestamped mood event of a consultant: sad, happy, neutral
  * */
case class Mood(event_time: Long, ingestion_time: Long, name: String, mood: String)

/**
  * result of joining a recommendation with the consultant's mood
  * */
case class MoodRec(eventTime: Long, consultant: String, mood: Option[String], recommendation: String)

/**
  * Stateful stream transformer plugged to 2 topics: consultants's mood and consultant's business recommendations.
  *
  * => tries to produce a streams of joined events in for the form of instances of MoodRec, resolving
  */
class EventTimeJoiner extends Transformer[String, Either[Recommendation, Mood], KeyValue[String, MoodRec]] {

  // how often do we review previously joined data?
  val reviewJoinPeriod = 1000

  // at each periodic review, we revisit all joined data before (now() - reviewLagDelta)
  val reviewLagDelta = 5000

  val BEGINNING_OF_TIMES = 0l

  // tons of mutable null values initialized a bit later, because java
  var ctx: ProcessorContext = _
  var moodStore: KeyValueStore[String, List[Mood]] = _
  var bestEffortJoinsStore: WindowStore[String, MoodRec] = _
  var consultantNamesStore : WindowStore[String, String] = _

  override def init(context: ProcessorContext): Unit = {
    ctx = context
    ctx.schedule(reviewJoinPeriod)

    moodStore = ctx.getStateStore("moods").asInstanceOf[KeyValueStore[String, List[Mood]]]
    bestEffortJoinsStore = ctx.getStateStore("bestEffortJoins").asInstanceOf[WindowStore[String, MoodRec]]
    consultantNamesStore  = ctx.getStateStore("consultants").asInstanceOf[WindowStore[String, String]]
  }

  /**
    * Main event handling:
    *  - if receiving a recommendation event: performs a best-effort join (based on why info is available now),
    *    emits the result and records the event in the windowStore
    *  - if receiving a mood event: just record it in key-value store
    *
    *  In all cases: also keep a trace of consultant's names we've encountered recently
    * */
  override def transform(key: String, event: Either[Recommendation, Mood]): KeyValue[String, MoodRec] =

    event match {
      case Left(rec) =>
        val joined = join(rec.event_time, rec.consultant, rec.recommendation)
        bestEffortJoinsStore.put(rec.consultant, joined, rec.event_time)
        consultantNamesStore.put("all-recent-names", rec.consultant, rec.event_time)
        new KeyValue(key, joined)

      case Right(mood) =>
        val updatedMoodHistory = (mood :: Option(moodStore.get(mood.name)).getOrElse(Nil)).sortBy( - _.event_time)
        moodStore.put(mood.name, updatedMoodHistory)
        consultantNamesStore.put("all-recent-names", mood.name, mood.event_time)
        null
    }


  /**
    * joins this recommendation with the latest mood known before it, if any
    * */
  def join(recommendationTime: Long, consultant: String, recommendation: String): MoodRec = {

   val maybeMood = Option(moodStore.get(consultant)).getOrElse(Nil)
    .dropWhile(_.event_time >= recommendationTime)
    .headOption
    .map(_.mood)

    MoodRec(recommendationTime, consultant, maybeMood, recommendation)
  }

  /**
    * set of consultants encountered recently, either in a mood event or a recommendation events
    * */
  def allRecentConsultants(until: Long): Iterator[String] =
    consultantNamesStore.fetch("all-recent-names", BEGINNING_OF_TIMES, until).asScala.map(_.value)


  /**
    * This is called every `reviewJoinPeriod` ms, it reviews previously joined events and re-emits if necessary
    * */
  override def punctuate(latestEventTime: Long): KeyValue[String, MoodRec] = {
    allRecentConsultants(until = latestEventTime).foreach {
      consultantName => joinAgain(consultantName, maxEventTimestamp = latestEventTime - reviewLagDelta)
    }
    null
  }

  /**
    * actual review of previously joined events of a consultant
    * */
  def joinAgain(consultantName: String, maxEventTimestamp: Long): Unit = {

    // joined data as per when the recommendation event was received
    val oldJoinedMoods = bestEffortJoinsStore
      .fetch(consultantName, BEGINNING_OF_TIMES, maxEventTimestamp)
      .asScala
      .map(_.value)
      .toList

    val newJoinedMoods = oldJoinedMoods
      .map(evt => join(evt.eventTime, evt.consultant, evt.recommendation))

    // if updated join is different: emit the new value
    (oldJoinedMoods zip newJoinedMoods)
      .filter{ case( MoodRec(_, _, oldMood, _), MoodRec(_, _, newMood, _)) => oldMood != newMood }
      .foreach{ case ( _ , updated ) => ctx.forward(consultantName, updated)}

    // TODO: if we update the joined value, we should record that in the windowed storage

    // TODO: explicitly evict old moods here (or use window store here as well?)

  }

  override def close(): Unit = {}
}

object EventTimeJoiner {
  val supplier = new TransformerSupplier[String, Either[Recommendation, Mood], KeyValue[String, MoodRec]] {
    override def get() = new EventTimeJoiner()
  }
}


object EventTimeJoinExample {

  /**
    * quick and dirty parser that does not handle parsing errors ^^
    * */
  def parse(rawJson: String): Option[Either[Recommendation, Mood]] =
    Recommendation.parseJson(rawJson) match  {
      case recommendation: JsSuccess[Recommendation] => Some(Left(recommendation.get))
      case e1: JsError =>
        MoodListJsonSerde.parseJsonMood(rawJson) match {
          case mood: JsSuccess[Mood] => Some(Right(mood.get))
          case e2: JsError => None
      }
  }

  /**
    * extract the user id out of any of those parsed events
    * */
  def userId(event: Either[Recommendation, Mood]): String =
    event match {
      case Left(rec) => rec.consultant
      case Right(mood) => mood.name
    }

}

object EventTimeJoinExampleApp extends App {

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, s"event-time-join-example-${System.currentTimeMillis()}")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    p
  }

  import org.apache.kafka.streams.state.Stores

  // store of (consultantName -> chronological-list-of-moods)
  val moodStore = Stores
    .create("moods")
    .withStringKeys
    .withValues(MoodListJsonSerde)
    .persistent()
    .build

  // window store of (consultantName -> (mood, recommendation)
  val bestEffortJoinStore = Stores
    .create("bestEffortJoins")
    .withStringKeys
    .withValues(MoodRecJsonSerde)
    .persistent()
    .windowed(1000, 100000, 10, false)
    .build

  // set of consultant names we encountered recently (only one hard-coded key)
  val consultantStore = Stores
    .create("consultants")
    .withStringKeys
    .withStringValues()
    .persistent()
    .windowed(1000, 100000, 10, false)
    .build

  val builder = new KStreamBuilder()

  builder.addStateStore(moodStore)
    .addStateStore(bestEffortJoinStore)
    .addStateStore(consultantStore).asInstanceOf[KStreamBuilder]

  // TODO: timestamp extractor is missing here
  builder
    .stream(Serdes.String(), Serdes.String(), "etj-events-2", "etj-moods-2")
    .mapValues[Option[Either[Recommendation, Mood]]](EventTimeJoinExample.parse)
    .filter{ case (_, v: Option[Either[Recommendation, Mood]]) =>  v.isDefined }
    .mapValues[Either[Recommendation, Mood]](_.get)
    .selectKey[String]{ case (_, v) => EventTimeJoinExample.userId(v)}
    .transform(EventTimeJoiner.supplier, "moods", "bestEffortJoins", "consultants")
    .print()

  new KafkaStreams(builder, config).start()

}

