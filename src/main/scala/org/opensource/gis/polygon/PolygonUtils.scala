package org.opensource.gis.polygon
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.mutable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * Source: http://alienryderflex.com/polygon/
 * Third algorithm with precalc optimization
 */

case class Diner(diner_id: Int, latitude: Double, longitude: Double)
case class GeoPoint(latitude: Double, longitude: Double)

case class Polygon(wkt: String) {
  val y  = wkt.replaceAll("POLYGON", "").replaceAll("\\(", "" ).replaceAll("\\)", "" ).split(",")

// polygon is arranged as LON/LAT - so reverse
  val points = y.map(_.split(' ') match {case Array(a,b) => GeoPoint(b.toDouble,a.toDouble)})
  def corners = points.size
  def horizontalCoordinates = points map (_.latitude)
  def verticalCoordinates = points map (_.longitude)
}

case class Str2Diners(points: String) {
    val y = points.replaceAll("\\[", "" ).replaceAll("\\]", "").split(",")
    val z  = y.map(_.trim.split(' ') match {case Array(a,b,c) => Diner(a.toInt, b.toDouble, c.toDouble)})
}


object PolygonUtils {
  def pointInPolygon(PolygonStr: String, points: String): Array[Int] = {

      @tailrec
      def precalc(polyCorners: Int, i: Int, j: Int, polyX: Array[Double], polyY: Array[Double],
                  constant: List[Double], multiple: List[Double]): (List[Double], List[Double])= {
        i match {
          case i if i == polyCorners => (constant, multiple)
          case i if polyY{j} == polyY{i} => precalc(polyCorners, i + 1, i, polyX, polyY, polyX{i} :: constant, 0d :: multiple)
          case i :Int => {
            val k = polyX{i} - (polyY{i} * polyX{j}) / (polyY{j} - polyY{i}) + (polyY{i} * polyX{i} ) / (polyY{j} - polyY{i})
            val m = (polyX{j} - polyX{i}) / (polyY{j} - polyY{i})
            precalc(polyCorners, i + 1, i, polyX, polyY, k :: constant, m :: multiple)
          }
        }
      }

      @tailrec
      def isInside(diner: Diner, polyCorners: Int, i: Int, j: Int, polyX: Array[Double], polyY: Array[Double],
                    constant: Array[Double], multiple: Array[Double], oddNodes: Boolean): Boolean = {
        val x = diner.latitude
        val y = diner.longitude
        i match {
          case i if i == polyCorners => oddNodes
          case i if polyY{i} < y && polyY{j} >= y || polyY{j} < y && polyY{i}>=y => {
            val odd = oddNodes ^ ( y * multiple{i} + constant{i} < x)
            isInside(diner, polyCorners, i + 1, i, polyX, polyY, constant, multiple, odd)
          }
          case i: Int => isInside(diner, polyCorners, i + 1, i, polyX, polyY, constant, multiple, oddNodes)
        }
      }

    val polygon = Polygon(PolygonStr)
    val polyX: Array[Double] = polygon.horizontalCoordinates
    val polyY: Array[Double] = polygon.verticalCoordinates

    val tuple = precalc(polygon.corners, 0, polygon.corners - 1, polyX, polyY, List(), List())

    val oddNodes = false

    val DinerList = Str2Diners(points).z

    val r =  DinerList.map(x=> if (isInside(x, polygon.corners, 0, polygon.corners - 1, polyX, polyY, tuple._1.toArray, tuple._2.toArray, oddNodes)) x.diner_id else 0).filter(x=> x>0)
    r
  }
  def scala_pip(): UserDefinedFunction = udf(pointInPolygon _ )
}
