package parallelai.spyglass.hbase

import java.io.File
import scala.collection.JavaConverters._
import com.google.common.io.Files
import cascading.pipe.Pipe
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.{ Tool, Job, Args, Csv }
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster
import org.apache.hadoop.hbase.{ HBaseConfiguration, MiniHBaseCluster, HTableDescriptor, HColumnDescriptor, TableExistsException }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Scan, Delete, Result }
import org.scalatest.{ FunSpec, BeforeAndAfterAll }

class LoadJob(args: Args) extends Job(args) {
  Csv("file://" + SpyGlassSpec.getLocalPath("test/candidates.txt").toString, separator = "|", skipHeader = true, quote = null)
    .then((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('first, 'last, 'party, 'rating)))
    .write(HBaseSource("load", "localhost:2181", ('first), List("cf", "cf", "cf"), List("last", "party", "rating")))
}

class CopyJob(args: Args) extends Job(args) {
  HBaseSource("load", "localhost:2181", ('first), List("cf", "cf", "cf"), List("last", "party", "rating"))
    .write(HBaseSource("copy", "localhost:2181", ('first), List("cf", "cf", "cf"), List("last", "party", "rating")))
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
}

class SpyGlassSpec extends FunSpec with BeforeAndAfterAll {
  import SpyGlassSpec._

  val conf = new HBaseConfiguration
  val tmpDir = Files.createTempDir
  val zkCluster = new MiniZooKeeperCluster
  conf.set("hbase.zookeeper.property.clientPort", zkCluster.startup(tmpDir).toString)
  val hbaseCluster = new MiniHBaseCluster(conf, 1)

  describe("An HBaseSource") {
    it("should be able to participate in a flow as a sink and source") {
      val loadTable = createTable(conf, "load", "cf")
      val copyTable = createTable(conf, "copy", "cf")
      val checkMap = Map(
          "joe" -> Map("last" -> "biden", "party" -> "democrats", "rating" -> "5"),
          "paul" -> Map("last" -> "ryan", "party" -> "", "rating" -> "3"))
      truncTable(loadTable)
      truncTable(copyTable)
      ToolRunner.run(conf, new Tool, Array("parallelai.spyglass.hbase.LoadJob", "--hdfs"))
      ToolRunner.run(conf, new Tool, Array("parallelai.spyglass.hbase.CopyJob", "--hdfs"))
      assert(toMaps(loadTable.getScanner(Bytes.toBytes("cf")).next(1000), "cf") == checkMap)
      assert(toMaps(copyTable.getScanner(Bytes.toBytes("cf")).next(1000), "cf") == checkMap)
    }
  }

  override def afterAll(configMap: Map[String, Any]) {
    hbaseCluster.shutdown
    hbaseCluster.join
  }

}

