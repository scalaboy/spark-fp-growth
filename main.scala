
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import scala.collection.mutable.Map
import org.apache.spark.SparkContext._

object Main {

  def main(args: Array[String]) {
    run(args)

  }
  def run(args: Array[String]) = {
    //最小支持率
    val supportRate = 0.85
    //将事务数据分到不同组中，每个组有相邻的20个item
    val everyGroupNum = 15
    //val conf = new SparkConf().setAppName("FP_Growth").setMaster("local[1]")
    val conf = new SparkConf().setAppName("FP_Growth")
    val sc = new SparkContext(conf)
    //读取hdfs上的源文件
    val originalTrans = sc.textFile(args(0))

    //去掉原始数据集的行号，并把相同行的数据进行合并
    val combinedTrans = originalTrans.map(line => (
      line.split(" ")
        .drop(1)
        .toList
        .sortWith(_ < _), 1))
      .reduceByKey(_ + _)


    //计算每个item的支持度,并把它们按照支持度从大到小生成一个数组
    val itemsSupport = combinedTrans.flatMap(line => {
      var l = List[(String, Int)]()
      for (i <- line._1) {
        l = (i, line._2) :: l
      }
      l
    })
      .reduceByKey(_ + _)
      // .sortBy(_._2, false) //disascend sort
      .map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
      .collect()


    var itemCount=0
    //将itemId映射为一个整形，以减少存储空间并增加计算效率
    val itemIntMap = Map[String, Int]()

    //支持度最高item映射为1，其它的以此加1
    for (i <- itemsSupport) {
      itemCount = itemCount + 1
      itemIntMap(i._1) = itemCount
    }


    //将记录的所有item换成整形，并按整形值从小到大排序，也就是按商品支持度从大到小排序
    val mappedTrans = combinedTrans.map(t => {
      var l = List[Int]()
      for (i <- t._1)
        l = itemIntMap(i) :: l
      (l.sortWith(_ < _), t._2)
    })


    //求出记录总数
    val transCount: Int = mappedTrans.map(t => (1, t._2))
      .reduceByKey(_ + _)
      .first()._2

    //求出最小支持度
    val minSupport=transCount*supportRate toInt

    //根据item总数和每个组item个数everyGrouNum求出共要分成多少个组
    val groupNum = (itemCount + everyGroupNum - 1) / everyGroupNum

    //将支持度连续的everyGroupNum的item分配到同一个group中
    val groupedTrans = mappedTrans.flatMap(t => {
      var pre = -1
      var i = t._1.length - 1
      var result = List[(Int, (List[Int], Int))]()
      while (i >= 0) {
        if ((t._1(i) - 1) / groupNum != pre) {
          pre = (t._1(i) - 1) / groupNum
          result = (pre, (t._1.dropRight(t._1.length - i - 1), t._2)) :: result
        }
        i -= 1
      }
      result
    })
      .groupByKey()
      .cache()


    //t key: groupId
    //t value: Iterable trans list
    val d_result = groupedTrans.flatMap(t => {
      fp_growth(t._2, minSupport, t._1 * groupNum + 1 to (((t._1 + 1) * groupNum) min itemCount))
    })

    //将d_result中的item 整形值转化为原来的itemId,以便输出最后结果
    val temp_result = d_result.map(t => (t._1.map(a => itemsSupport(a - 1)._1), t._2))
    //将挖掘后的结果根据项数分配到不同的group中以便生成到不同的文件中
    val result = temp_result.map( t => ( listToString(t._1)._2, listToString(t._1)._1 + ":" + t._2.toDouble/transCount)).groupBy(_._1)
    result.map(t => t._2.map(s => s._2)).saveAsTextFile(args(1))
    sc.stop()

    //格式化生成的结果
    val arr=args(1).split(":")
    val str=arr(arr.length-1)
    val index=str.indexOf("/")
    //获取nameNode后面的文件路径
    val prePath=str.substring(index)
    //获取nameNode的uri
    val nameNodePath=args(1).substring(0,args(1).length-prePath.length)
    ResultFormat.format(nameNodePath,prePath)
  }

  //将item List合并成一个String 并求出该List的个数，即求出频繁项数
  def listToString(l: List[String]): (String, Int) = {
    var str = ""
    var count = 0
    for (i <- l) {
      str += i + ","
      count += 1
    }
    str = "\n"+str.substring(0, str.size - 1)
    return (str, count)
  }

  /** fp-TreeNode' growth
  contains make TreeNode,prefix cut,deal with target,deal with single branche and mining frequent itemset
    */
  def fp_growth(v: Iterable[(List[Int], Int)], minSupport: Int, target: Iterable[Int] = null): List[(List[Int], Int)] = {

    val root = new TreeNode(null, null, 0)
    //表头
    val tab = Map[Int, TreeNode]()
    //记录树中同一个itemId的支持度之和
    val tabc = Map[Int, Int]()
    //make TreeNode
    for (i <- v) {
      var cur = root;
      var s: TreeNode = null
      var list = i._1
      while (!list.isEmpty) {
        if (!tab.exists(_._1 == list(0))) {
          tab(list(0)) = null
        }
        if (!cur.son.exists(_._1 == list(0))) {
          s = new TreeNode(cur, tab(list(0)), list(0))
          tab(list(0)) = s
          cur.son(list(0)) = s
        } else {
          s = cur.son(list(0))
        }
        s.support += i._2
        cur = s
        list = list.drop(1)

      }
    }
    //prefix cut
    for (i <- tab.keys) {
      var count = 0
      var cur = tab(i)
      while (cur != null) {
        count += cur.support
        cur = cur.Gnext
      }
      //modify
      tabc(i) = count
      if (count < minSupport) {
        var cur = tab(i)
        while (cur != null) {
          var s = cur.Gnext
          cur.Gparent.son.remove(cur.Gv)
          cur = s
        }
        tab.remove(i)
      }
    }
    //deal with target
    var r = List[(List[Int], Int)]()
    var tail: Iterable[Int] = null
    if (target == null)
      tail = tab.keys
    else {
      tail = target.filter(a => tab.exists(b => b._1 == a))
    }
    if (tail.count(t => true) == 0)
      return r
    //deal with the single branch
    var cur = root
    var c = 1
    while (c < 2) {
      c = cur.son.count(t => true)
      if (c == 0) {
        var res = List[(Int, Int)]()
        while (cur != root) {
          res = (cur.Gv, cur.support) :: res
          cur = cur.Gparent
        }

        val part = res.partition(t1 => tail.exists(t2 => t1._1 == t2))
        val p1 = gen(part._1)
        if (part._2.length == 0)
          return p1
        else
          return decare(p1, gen(part._2)) ::: p1
      }
      cur = cur.son.values.head
    }
    //mining the frequent itemset
    for (i <- tail) {
      var result = List[(List[Int], Int)]()
      var cur = tab(i)
      while (cur != null) {
        var item = List[Int]()
        var s = cur.Gparent
        while (s != root) {
          item = s.Gv :: item
          s = s.Gparent
        }
        result = (item, cur.support) :: result
        cur = cur.Gnext
      }
      r = (List(i), tabc(i)) :: fp_growth(result, minSupport).map(t => (i :: t._1, t._2)) ::: r

    }
    r
  }

  def gen(tab: List[(Int, Int)]): List[(List[Int], Int)] = {
    if (tab.length == 1) {
      return List((List(tab(0)._1), tab(0)._2))
    }
    val sp = tab(0)
    val t = gen(tab.drop(1))
    return (List(sp._1), sp._2) :: t.map(s => (sp._1 :: s._1, s._2 min sp._2)) ::: t
    //TODO: sp._2 may not be min
  }

  //笛卡尔积
  def decare[T](a: List[(List[T], Int)], b: List[(List[T], Int)]): List[(List[T], Int)] = {
    var res = List[(List[T], Int)]()
    for (i <- a)
      for (j <- b)
        res = (i._1 ::: j._1, i._2 min j._2) :: res
    res
  }
}

class TreeNode(parent: TreeNode, next: TreeNode, v: Int) {
  val son = Map[Int, TreeNode]()
  var support = 0
  def Gparent = parent
  def Gv = v
  def Gnext = next
}

