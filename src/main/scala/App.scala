
import com.autohom.www.MyUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object processdata extends App {
  def indiceChange(sc: SparkContext, path_in: String, sep: String): RDD[String] = {
    """
    """.stripMargin
    val data = sc.textFile(path_in,minPartitions = 500).cache()
    val train: RDD[String] = data.map {
      line =>

        val segs: Array[String] = if (sep == "1") {
          line.split(' ')
        } else {
          line.split('\t')
        }
        val label = if (segs(0).toInt >= 1) "1" else "0"
        val features = segs.drop(1)
        // add indices 1
        val features_process: Array[String] = features.map {
          elem =>
            val index = elem.split(":")(0).toInt
            val value = elem.split(":")(1)
            val new_index = index + 1 //index should be begin 1
            //val new_index = index
            new_index.toString + ":" + value
        }
        // sort index
        val features_sort: Array[String] = features_process.sortWith {
          (leftE, rightE) =>
            leftE.split(":")(0).toInt < rightE.split(":")(0).toInt
        }
        val line_arr: Array[String] = label +: features_sort
        // string line
        line_arr.mkString(" ")
    }
    train
  }

  def process_data(sc: SparkContext, path_in: String, ifSplit: Double, part: Int, sep: String): Array[RDD[String]] = {


    val train: RDD[String] = indiceChange(sc, path_in, sep)
    val util = new MyUtil
    val splitRdd: Array[RDD[String]] = train.randomSplit(Array(10 * ifSplit, 10 * (1 - ifSplit)), 2017)
    return splitRdd
  }

  override def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger("MY LOGGER")
    logger.info(s"===args===")
    args.foreach(elem => logger.info(elem))
    logger.info(s"===args===")

    val train_path_in = "/team/ad_wajue/chenlongzhen/train_mobai"

    // print warn
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("clz processing")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    conf.set("spark.kryoserializer.buffer.max", "2047m")

    val sc: SparkContext = new SparkContext(conf)
    sc.setCheckpointDir("/team/ad_wajue/chenlongzhen/checkpoint")


    logger.info("processing data")

    val splitdata: Array[RDD[LabeledPoint]] = process_data(sc, train_path_in, 0.8, 500, "1")

    val train: RDD[LabeledPoint] = splitdata(0)
    val test = splitdata(1)
    train.saveAsTextFile("/team/ad_wajue/chenlongzhen/train_moba.train")
    test.saveAsTextFile("/team/ad_wajue/chenlongzhen/train_moba.test")
  }
}
