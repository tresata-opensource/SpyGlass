package parallelai.spyglass.hbase

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import com.google.common.io.Files
import cascading.pipe.Pipe
import cascading.tuple.{ Tuple => CTuple, TupleEntry }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.{ Job, Args, Tsv, JobTest }
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{ HBaseConfiguration, MiniHBaseCluster, HTableDescriptor, HColumnDescriptor, TableExistsException }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Scan, Delete, Result }
import org.scalatest.{ FunSpec, BeforeAndAfterAll }

class LoadJob(args: Args) extends Job(args) {
  Tsv("input", ('fruit, 'descr, 'comment))
    .then((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('fruit, 'descr, 'comment)))
    .write(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))
}

class ExtractJob(args: Args) extends Job(args) {
  HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment"))
  .then((pipe: Pipe) => new HBasePipeWrapper(pipe).fromBytesWritable(('fruit, 'descr, 'comment)))
  .write( Tsv("output", ('fruit, 'descr, 'comment)))
}

object SpyGlassSpec {
  def createTable(conf: HBaseConfiguration, tableName: String = "test", family: String = "cf"): HTable = {
    val desc = new HTableDescriptor(Bytes.toBytes(tableName))
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(family)))
    try {
      new HBaseAdmin(conf).createTable(desc)
    } catch {
      case e: TableExistsException => Unit
    }
    new HTable(conf, tableName)
  }

  def truncTable(table: HTable) {
    for(res <- table.getScanner(new Scan).asScala) table.delete(new Delete(res.getRow))
  }

  def resultToMap(result: Result, family: String = "cf"): Map[String, String] =
    Map() ++ result.getFamilyMap(Bytes.toBytes(family)).asScala.iterator.map{ case (key, value) => (Bytes.toString(key), Bytes.toString(value)) }

  def resultsToMaps(results: Array[Result], family: String = "cf"): Map[String, Map[String, String]] =
    results.map{ result => (Bytes.toString(result.getRow), resultToMap(result, family)) }.toMap

  def tableToMaps(table: HTable, family: String = "cf"): Map[String, Map[String, String]] =
    resultsToMaps(table.getScanner(Bytes.toBytes(family)).next(10000))

  def teToMap(te: TupleEntry): Map[String, String] =
    te.getFields.iterator.asScala.zip(te.getTuple.iterator.asScala)
      .filter{ case (field, value) => value != null }
      .map{ case (field, value) => (field.toString, value.toString) }
      .toMap

  def noOp[A](buffer: Buffer[A]) {}
}

class SpyGlassSpec extends FunSpec with BeforeAndAfterAll {
  import SpyGlassSpec._
  import com.twitter.scalding.Dsl._

  val conf = new HBaseConfiguration
  val tmpDir = Files.createTempDir
  val zkCluster = new MiniZooKeeperCluster
  zkCluster.setDefaultClientPort(2181)
  val clientPort = zkCluster.startup(tmpDir)
  conf.set("hbase.zookeeper.property.clientPort", clientPort.toString)
  val hbaseCluster = new MiniHBaseCluster(conf, 1)
  val htable = createTable(conf)

  val testData = List(("apple", "granny smith", "from long island"), ("pear", "barlett", null.asInstanceOf[String]))
  val checkMap = Map(
    "apple" -> Map("descr" -> "granny smith", "comment" -> "from long island"),
    "pear" -> Map("descr" -> "barlett")
  )

  describe("An HBaseSource") {
    it("should be able to participate in a flow as a sink") {
      truncTable(htable)

      JobTest("parallelai.spyglass.hbase.LoadJob")
        .source(Tsv("input", ('fruit, 'descr, 'comment)), testData)
        .sink(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))(noOp[(String, String, String)])
        .runHadoop
        .finish
      assert(checkMap == tableToMaps(htable))

      JobTest("parallelai.spyglass.hbase.ExtractJob")
        .source(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")), testData)
        .sink(Tsv("output", ('fruit, 'descr, 'comment))){ buffer: Buffer[CTuple] => {
          assert(buffer.forall{ tup => {
            val m = teToMap(new TupleEntry(('fruit, 'descr, 'comment), tup))
            checkMap.contains(m("fruit")) && checkMap(m("fruit")) == m - "fruit"
          }})
        }}
        .runHadoop
        .finish
    }
  }

  override def afterAll(configMap: Map[String, Any]) {
    hbaseCluster.shutdown
    hbaseCluster.join
  }

}

