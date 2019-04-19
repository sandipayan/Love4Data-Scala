import org.apache.spark.sql.Row

import scala.collection.mutable

val r1 = Row(19920023,40.06823348,-75.07845307)
val r2 = Row(19920477,34.17946243,-118.5388946)
val r3 = Row(19920477,34.17946243,-118.5388946)

val DinerList : mutable.WrappedArray[Row] = mutable.WrappedArray.make(Array(r1,r2,r3))

//val s: mutable.WrappedArray[Int] = mutable.WrappedArray.make(Array(1,2,3))

case class Diner(diner_id: Int, latitude: Double, longitude: Double)

//val y: mutable.WrappedArray[Int] = mutable.WrappedArray.make(Array(1,2,3))

//val a4= s.map {
//  case Row(diner_id: Int, latitude: Double, longitude: Double) => Diner(diner_id: Int, latitude: Double, longitude: Double)
//}.map(x=> x.diner_id).toArray

val r =  DinerList.map{case Row(diner_id: Int, latitude: Double, longitude: Double) => Diner(diner_id: Int, latitude: Double, longitude: Double)}.map(x=> if (2>1) x.diner_id else 0).filter(x=> x>0).toArray

