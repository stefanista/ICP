import org.apache.spark.{SparkConf, SparkContext}

object SparkMatrixMultiplication {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val matrix1data=sc.textFile("data/A")
    val matrix2data=sc.textFile(path = "data/B")

    //gets file a and places it in a matrix 1
    val matrix1=matrix1data.zipWithIndex()
      .flatMap{
      case(line,i)=>{
        var j =0
        val linesplit=line.split(" ")
        linesplit.map(f=>{
          j = j+1
          (i+1,j.toLong,f.toInt)
        })
    }}

    //uses file b and creates matrix 2
    val matrix2=matrix2data.zipWithIndex()
      .flatMap{
        case(line,i)=>{
          var j =0
          val linesplit=line.split(" ")
          linesplit.map(f=>{
            j = j+1
            (i+1,j.toLong,f.toInt)
          })
        }}
    //the tuple is i row, j column, f value

    val lastelement1=matrix1.top(1)
    val lastelement2=matrix2.top(1)
    var ab = scala.collection.mutable.Map[String, String]()
    var keyPosition = ""
    var op = 0
    var mValue = ""

    //if it is a valid matrix so m X n n X p check is n and n are the same
    if(lastelement1(0)._2==lastelement2(0)._1)
      {
        println("Matrix A with " + lastelement1(0)._1 +" X "+lastelement1(0)._2)
        println("Matrix B with " + lastelement2(0)._1 +" X "+lastelement2(0)._2)
        println("Are compatible for Multiplication")
        /*
        for (a <- matrix1) {
          for ( b <- matrix2){
            if (a._1 == b._2  && a._2 == b._1){
              keyPosition = "" + a._1 +""+a._2
              op = a._3 * b._3
              mValue = "" + op + ""
              ab += (keyPosition -> mValue)
            }
          }
        }
        */

        val mm=matrix1.union(matrix2)
        //put matrix 1 and 2 in arrays
        var aMatrix = matrix1.top(matrix1.count().toInt)
        var bMatrix = matrix2.top(matrix2.count().toInt)
        //var bothMatrix = mm.top(mm.count().toInt)

        //go through for the i dimension
        for (i <- 1 to lastelement1(0)._1.toInt) {
          //go through the j dimension
          for ( j <- 1 to lastelement2(0)._2.toInt){
            //go through the i values in matrix 1
            for ( aIndex <- 0 to matrix1.count().toInt-1) {

              var a = aMatrix(aIndex)
              //go through the j values in matrix 2
              for (bIndex <- 0 to matrix2.count().toInt-1) {

                var b = bMatrix(bIndex)

                //ij += matrix1 at ij * matrix 2 at ji
                if (a._1 == i && a._2 == j && b._1 == j && b._2 == i) {

                  keyPosition = "" + i + "" + j
                  op = a._3 * b._3
                  mValue = "" + op + ""
                  ab += (keyPosition -> mValue)

                  /*
                  for (i to m)   a has dim m by n b has dim n by p
                    for (j to p)
                      key position ij += aij * bji


                   */

                }
              }
            }
          }
        }

        //print
        println(ab)
        //used for debugging
        var e = 0

      }
    else
      {
        println("Matrix A with " + lastelement1(0)._2 +" X "+lastelement1(0)._3)
        println("Matrix B with " + lastelement2(0)._2 +" X "+lastelement2(0)._3)
        println("Are NOT compatible for Multiplication")
      }

  }

}
