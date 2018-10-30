import org.apache.spark.sql.SparkSession

object MergeSort {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\Winutils");

    def merge(leftList: List[Int], rightList: List[Int]): List[Int] = (leftList, rightList) match {
      case (Nil, _) => rightList
      case (_, Nil) => leftList
      case (l :: lList, r :: rList) =>
        if (l <= r) l :: merge(lList, rightList)
        else r :: merge(rList, leftList)
    }

    def mergeSort(lst: List[Int]): List[Int] = {
      if (lst.length < 2) lst
      else {
        val (leftSide, rightSide) = lst.splitAt(lst.length / 2)
        merge(mergeSort(leftSide), mergeSort(rightSide))
      }
    }

    val nums = List.fill(10)(util.Random.nextInt(100))
    println(nums.mkString(", "))
    println(mergeSort(nums).mkString(", "))

  }
}
