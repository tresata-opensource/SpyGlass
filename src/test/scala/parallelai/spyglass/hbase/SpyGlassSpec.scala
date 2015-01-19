package parallelai.spyglass.hbase

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import cascading.pipe.Pipe
import cascading.tuple.{ Tuple => CTuple, TupleEntry }
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.{ Job, Args, Tsv, JobTest }
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor, HBaseTestingUtility, TableName }
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTableInterface, Scan, Delete, Result, HConnectionManager }
import org.scalatest.{ FunSpec, BeforeAndAfterAll }

class LoadJob(args: Args) extends Job(args) {
  Tsv("input", ('fruit, 'descr, 'comment))
    .thenDo((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('fruit, 'descr, 'comment)))
    .write(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))
}

class LoadOpsJob(args: Args) extends Job(args) {
  Tsv("input", ('fruit, 'descr, 'comment))
    .thenDo((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('fruit)))
    .thenDo((pipe: Pipe) => new HBasePipeWrapper(pipe).toHBaseOperation(('descr, 'comment)))
    .write(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))
}

class ExtractJob(args: Args) extends Job(args) {
  HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment"))
  .thenDo((pipe: Pipe) => new HBasePipeWrapper(pipe).fromBytesWritable(('fruit, 'descr, 'comment)))
  .write(Tsv("output", ('fruit, 'descr, 'comment)))
}

object SpyGlassSpec {
  def resultToMap(result: Result, family: String = "cf"): Map[String, String] =
    Map() ++ result.getFamilyMap(Bytes.toBytes(family)).asScala.iterator.map{ case (key, value) => (Bytes.toString(key), Bytes.toString(value)) }

  def resultsToMaps(results: Array[Result], family: String = "cf"): Map[String, Map[String, String]] =
    results.map{ result => (Bytes.toString(result.getRow), resultToMap(result, family)) }.toMap

  def tableToMaps(table: HTableInterface, family: String = "cf"): Map[String, Map[String, String]] =
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

  val htu = HBaseTestingUtility.createLocalHTU
  val conf = htu.getConfiguration
  conf.set("test.hbase.zookeeper.property.clientPort", "2181")
  htu.cleanupTestDir()
  val zkMiniCluster = htu.startMiniZKCluster()
  htu.startMiniHBaseCluster(1, 1)
  val conn = HConnectionManager.createConnection(conf)
  val admin = new HBaseAdmin(conf)

  override def afterAll() {
    conn.close()
    admin.close()
    htu.shutdownMiniHBaseCluster()
    htu.shutdownMiniZKCluster()
    htu.cleanupTestDir()
  }

  def createTable(tableName: String = "test", family: String = "cf"): HTableInterface = {
    val desc = new HTableDescriptor(TableName.valueOf(tableName))
    desc.addFamily(
      new HColumnDescriptor(Bytes.toBytes(family))
        .setBloomFilterType(BloomType.NONE)
        .setMaxVersions(3)
    )
    if (admin.tableExists(tableName)) {
      admin.disableTable("test")
      admin.deleteTable("test")
    } 
    admin.createTable(desc)
    conn.getTable(tableName)
  }

  def truncTable(table: HTableInterface) {
    for(res <- table.getScanner(new Scan).asScala) table.delete(new Delete(res.getRow))
  }

  val htable = createTable()

  val testData = List(
    ("apple", "granny smith", "from long island"),
    ("pear", "barlett", null.asInstanceOf[String])
  )
  val checkMap = Map(
    "apple" -> Map("descr" -> "granny smith", "comment" -> "from long island"),
    "pear" -> Map("descr" -> "barlett")
  )
  val testDataOps = List(
    ("apple", null.asInstanceOf[String], "__DELETE_COLUMN__"),
    ("pear", "__DELETE_ROW__", null.asInstanceOf[String]),
    ("orange", "valencia", "from italy")
  )
  val checkMapOps = Map(
    "apple" -> Map("descr" -> "granny smith"),
    "orange" -> Map("descr" -> "valencia", "comment" -> "from italy")
  )

  describe("An HBaseSource") {
    it("should be able to participate in a flow as a source or sink") {
      truncTable(htable)

      JobTest("parallelai.spyglass.hbase.LoadJob")
        .source(Tsv("input", ('fruit, 'descr, 'comment)), testData)
        .sink(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))(noOp[(String, String, String)])
        .runHadoop
        .finish
      assert(checkMap === tableToMaps(htable))

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

      JobTest("parallelai.spyglass.hbase.LoadOpsJob")
        .source(Tsv("input", ('fruit, 'descr, 'comment)), testDataOps)
        .sink(HBaseSource("test", "localhost:2181", ('fruit), List("cf", "cf"), List("descr", "comment")))(noOp[(String, String, String)])
        .runHadoop
        .finish
      assert(checkMapOps === tableToMaps(htable))
    }
  }
}
