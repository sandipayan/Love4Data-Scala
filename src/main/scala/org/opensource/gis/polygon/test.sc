//import org.apache.spark.sql.Row
//
//import scala.collection.mutable
//
//val r1 = Row(19920023,40.06823348,-75.07845307)
//val r2 = Row(19920477,34.17946243,-118.5388946)
//val r3 = Row(19920477,34.17946243,-118.5388946)
//
//val DinerList : mutable.WrappedArray[Row] = mutable.WrappedArray.make(Array(r1,r2,r3))
//
////val s: mutable.WrappedArray[Int] = mutable.WrappedArray.make(Array(1,2,3))
//
//case class Diner(diner_id: Int, latitude: Double, longitude: Double)
//
////val y: mutable.WrappedArray[Int] = mutable.WrappedArray.make(Array(1,2,3))
//
////val a4= s.map {
////  case Row(diner_id: Int, latitude: Double, longitude: Double) => Diner(diner_id: Int, latitude: Double, longitude: Double)
////}.map(x=> x.diner_id).toArray
//
//val r =  DinerList.map{case Row(diner_id: Int, latitude: Double, longitude: Double) => Diner(diner_id: Int, latitude: Double, longitude: Double)}.map(x=> if (2>1) x.diner_id else 0).filter(x=> x>0).toArray
//
//
//case class Point(x: Double, y: Double)
//
val s = "[45647897 40.11610031 -88.23402405, 28346532 40.08257675 -88.25135041, 15610897 40.12036514 -88.24258423, 15956558 40.09732437 -88.24887085, 22237201 40.11902999 -88.23291016, 48291917 40.09290695 -88.23718262, 47991314 40.11040496 -88.23565674, 46006074 40.11053466 -88.23428345, 45320836 40.13845825 -88.24571229, 2142001 40.14471817 -88.24463654]"

val z = s.replaceAll("\\[", "" ).replaceAll("\\]", "").split(", ")

val a = z.map(_.trim())