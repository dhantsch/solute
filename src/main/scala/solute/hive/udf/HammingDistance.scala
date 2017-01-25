package solute.hive.udf

// import org.apache.spark.sql.expressions.Window
// import org.apache.spark.sql.functions._

object HammingDistance {

  // Calculate a sum of set bits of XOR'ed bytes
  def hammingDistance(b1: Array[Byte], b2: Array[Byte]): Int =
    b1.zip(b2).map((x: (Byte, Byte)) => numberOfBitsSet((x._1 ^ x._2).toByte)).sum


  // 1 iteration for each bit, 8 total. Shift right and AND 1 to get i-th bit
  def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

  /**
    * Calulate the hamming distance between two strings.
    *
    * @param s1 the first string
    * @param s2 the second string
    * @return the hamming distance
    */
  def hammingDistance(s1: String, s2: String): Int = s1.zip(s2).count(c => c._1 != c._2)

  /**
    * Calculate the hamming distance between the value and an list of values.
    *
    * @param value the current value
    * @param parents all parent values
    */
  def hammingDistance(value: String, parents: Array[String]): Array[Int] = {
    parents.map(x  => x.zip(value).count(c => c._1 != c._2))
  }

  /**
    * Calculate the min hamming distance for every array items.
    *
    * @param values the collection of strings
    * @return the haming distances for every items in the collection
    */
  def minHammingDistance(values: Array[String]) : Int = {
    val result = new Array[Int](values.length)
    val indexed = values.zipWithIndex

    for (x <- indexed) {
      val ax = values.slice(0, x._2)

      if (x._2 == 0) {
        result(x._2) = -1
      } else {
        result(x._2) = hammingDistance(x._1, ax).min
      }
    }
    // println (result.mkString(" "))

    result.filter(_ >= 0 ).min
  }
}