/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Darel Rex Finley, Sergio Magnacco
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */


package org.opensource.gis.polygon
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.mutable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * Source: http://alienryderflex.com/polygon/
 *
 * Third algorithm with precalc optimization
 */

case class Diner(diner_id: Int, latitude: Double, longitude: Double)
case class GeoPoint(latitude: Double, longitude: Double)

case class Polygon(wkt: String) {
  val y  = wkt.replaceAll("POLYGON", "").replaceAll("\\(", "" ).replaceAll("\\)", "" ).split(",")

  val z1 = y.map(_.split(' ')(0).toDouble)
  val z2 = y.map(_.split(' ')(1).toDouble)

  // polygon is arranged as LON/LAT - so reverse
  val zipped = z2 zip z1
  val points = zipped.map(GeoPoint.tupled).toList

  def corners = points.size
  def horizontalCoordinates = points map (_.latitude)
  def verticalCoordinates = points map (_.longitude)
}

object PolygonUtils {
  def pointInPolygon(PolygonStr: String, DinerList: mutable.WrappedArray[Row]): Array[Int] = {

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
    val polyX: Array[Double] = polygon.horizontalCoordinates.toArray
    val polyY: Array[Double] = polygon.verticalCoordinates.toArray

    val tuple = precalc(polygon.corners, 0, polygon.corners - 1, polyX, polyY, List(), List())

    val oddNodes = false

    // We have to have a numeric return in the else, else it goes to AnyRef data type
      val r =  DinerList.map{case Row(diner_id: Int, latitude: Double, longitude: Double) => Diner(diner_id: Int, latitude: Double, longitude: Double)}.map(x=> if (isInside(x, polygon.corners, 0, polygon.corners - 1, polyX, polyY, tuple._1.toArray, tuple._2.toArray, oddNodes)) x.diner_id else 0).filter(x=> x>0).toArray
      r
  }
  def scala_pip(): UserDefinedFunction = udf(pointInPolygon _ )
}
