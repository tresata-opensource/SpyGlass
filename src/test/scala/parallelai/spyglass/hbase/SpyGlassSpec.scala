package parallelai.spyglass.hbase

import java.io.File
import cascading.pipe.Pipe
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.hbase.util.Bytes
import com.twitter.scalding.{ Tool, Job, Args, Csv }
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.scalatest.{ FunSpec, BeforeAndAfterAll }

class TestJob1(args: Args) extends Job(args) {
  Csv("file://" + SpyGlassSpec.getLocalPath("test/candidates.txt").toString, separator = "|", skipHeader = true, quote = null)
    .then((pipe: Pipe) => new HBasePipeWrapper(pipe).toBytesWritable(('first, 'last, 'party, 'rating, 'score)))
    .write(new HBaseSource("test", "localhost:2181", ('first), List("cf", "cf", "cf", "cf"), List("last", "party", "rating", "score")))
}

object SpyGlassSpec {
  def getLocalPath(fileString: String): Path = new Path((new File(fileString).getAbsolutePath()).toString())
  //def resultToMap(result: Result): Map[String, Map[String, String]] =
}

class SpyGlassSpec extends FunSpec with BeforeAndAfterAll {

  val htest = new HBaseTestingUtility
  htest.startMiniCluster
  htest.startMiniMapReduceCluster
  val htable = htest.createTable(Bytes.toBytes("test"), Bytes.toBytes("cf"))

  describe("An HBaseSource") {
    it("should be able to participate in a flow as a source or sink") {
      htest.truncateTable(Bytes.toBytes("test"))
      ToolRunner.run(htest.getConfiguration, new Tool, Array("parallelai.spyglass.hbase.TestJob1", "--hdfs"))
      htable.getScanner(Bytes.toBytes("cf")).next(100).foreach{ result =>
        println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + result)
      }
    }
  }


  override def afterAll(configMap: Map[String, Any]) {
    //htest.shutdownMiniMapReduceCluster
    //htest.shutdownMiniCluster
  }

}

