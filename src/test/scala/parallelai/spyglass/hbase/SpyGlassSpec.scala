package parallelai.spyglass.hbase

import java.io.File
import scala.collection.JavaConverters._
import com.google.common.io.Files
import cascading.pipe.Pipe
import cascading.tuple.{ TupleEntry, Fields }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.{ Tool, Job, Args, Csv, Hdfs, Read }
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{ HBaseConfiguration, MiniHBaseCluster, HTableDescriptor, HColumnDescriptor, TableExistsException }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Scan, Delete, Result }
import org.scalatest.{ FunSpec, BeforeAndAfterAll }

class LoadJob(args: Args) extends Job(args) {
  Csv("file://" + SpyGlassSpec.getLocalPath("test/candidates.txt").toString, separator = "|", skipHeader = true, quote = null)
    .then((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('first, 'last, 'party, 'rating)))
    .write(HBaseSource("test", "localhost:2181", ('first), List("cf", "cf", "cf"), List("last", "party", "rating")))
}

class ExtractJob(args: Args) extends Job(args) {
  HBaseSource("test", "localhost:2181", ('first), List("cf", "cf", "cf"), List("last", "party", "rating"))
    .then((pipe: Pipe) => new HBasePipeWrapper(pipe).fromBytesWritable(('first, 'last, 'party, 'rating)))
    .write(Csv("file://" + SpyGlassSpec.getLocalPath("tmp/candidates").toString, separator = "|", skipHeader = true, quote = null))
}

object SpyGlassSpec {
  def getLocalPath(fileString: String): Path = new Path((new File(fileString).getAbsolutePath()).toString())

  def createTable(conf: HBaseConfiguration, tableName: String, family: String): HTable = {
    val desc = new HTableDescriptor(Bytes.toBytes(tableName))
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(family)))
    try {
      new HBaseAdmin(conf).createTable(desc)
    } catch {
      case e: TableExistsException => 1
    }
    new HTable(conf, tableName)
  }

  def truncTable(table: HTable) {
    for(res <- table.getScanner(new Scan).asScala) table.delete(new Delete(res.getRow))
  }

  def toMap(result: Result, family: String): Map[String, String] =
    Map() ++ result.getFamilyMap(Bytes.toBytes(family)).asScala.iterator.map{ case (key, value) => (Bytes.toString(key), Bytes.toString(value)) }

  def toMaps(results: Array[Result], family: String): Map[String, Map[String, String]] =
    results.map{ result => (Bytes.toString(result.getRow), toMap(result, family)) }.toMap

  def toMap(te: TupleEntry): Map[String, String] = (te.getFields.iterator.asScala zip te.getTuple.iterator.asScala)
    .filter{ case (field, item) => item != null }
    .map{ case (field, item) => (field.toString, item.toString) }
    .toMap
}

class SpyGlassSpec extends FunSpec with BeforeAndAfterAll {
  import SpyGlassSpec._

  val conf = new HBaseConfiguration
  val mode = Hdfs(true, conf)
  val tmpDir = Files.createTempDir
  val zkCluster = new MiniZooKeeperCluster
  conf.set("hbase.zookeeper.property.clientPort", zkCluster.startup(tmpDir).toString)
  val hbaseCluster = new MiniHBaseCluster(conf, 1)
  val table = createTable(conf, "test", "cf")

  val checkSet = Set(
    Map("first" -> "joe", "last" -> "biden", "party" -> "democrats", "rating" -> "5"),
    Map("first" -> "paul", "last" -> "ryan", "rating" -> "3")
  )
  val checkMap = checkSet.map(x => (x("first"), x - "first")).toMap

  describe("An HBaseSource") {
    it("should be able to participate in a flow as a sink and source") {
      truncTable(table)
      ToolRunner.run(conf, new Tool, Array("parallelai.spyglass.hbase.LoadJob", "--hdfs"))
      ToolRunner.run(conf, new Tool, Array("parallelai.spyglass.hbase.ExtractJob", "--hdfs"))
      assert(toMaps(table.getScanner(Bytes.toBytes("cf")).next(1000), "cf") == checkMap)
      assert(
        mode.openForRead(
          Csv("file://" + SpyGlassSpec.getLocalPath("tmp/candidates").toString, fields = new Fields("first", "last", "party", "rating"), separator = "|", quote = null)
            .createTap(Read)(mode)
        ).asScala.forall{ te => checkSet.contains(toMap(te)) }
      )
    }
  }

  override def afterAll(configMap: Map[String, Any]) {
    hbaseCluster.shutdown
    hbaseCluster.join
  }

}

