package org.opensource.gis.polygon

import com.sun.tools.javac.code.Attribute
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.Row

import scala.collection.mutable

class VerifyPointInPolygonUsingWGS84Format extends FlatSpec with Matchers {



  "A given point inside Retiro Neighborhood in Buenos Aires" should "pointInPolygon return true" in {

    val PolygonStr = "POLYGON((-118.62075033413201 34.19218805133938,-118.62074847895609 34.19218952967484,-118.6207469 34.192191300000005,-118.6166275969792 34.197683704027725,-118.59465793746526 34.21965326355551,-118.5781815727871 34.22514541844189,-118.57817863975359 34.22514679089539,-118.57817610050506 34.22514880050507,-118.56170070101012 34.2416242,-118.55621919898988 34.2416242,-118.55073009949494 34.23613510050507,-118.55072797798327 34.23613335942543,-118.55072555756806 34.23613206568655,-118.55072293126452 34.23613126900607,-118.5507202 34.236131,-118.5452271 34.236131,-118.54522436873549 34.23613126900607,-118.54522174243195 34.23613206568655,-118.54521932201673 34.23613335942543,-118.54521720050506 34.23613510050507,-118.53972633745882 34.24162596355131,-118.53149410002686 34.244370042695294,-118.52326196256021 34.241625963570336,-118.51777109949494 34.23613510050507,-118.51776856021388 34.236133090875285,-118.51776562714036 34.23613171841771,-118.50128926252218 34.23063966353229,-118.48481209949495 34.21416250050507,-118.48480956025726 34.21416049090209,-118.48480662723709 34.21415911844995,-118.47357190053079 34.21041416409827,-118.47381672285353 34.204247446981974,-118.4738239850086 34.20409064771541,-118.47382399776042 34.20409025040716,-118.4739239977604 34.198500250407164,-118.47396110096344 34.19635290268291,-118.47416640724302 34.184847871064605,-118.47416615000716 34.18484493842605,-118.47416528520554 34.184842124415155,-118.47416385107712 34.18483955345914,-118.46815884208776 34.176323377546,-118.46986597736124 34.16305850684076,-118.4701338838351 34.160991799756395,-118.4701339580405 34.16098891689999,-118.47013344000001 34.160986079999994,-118.47011969035886 34.16093893837321,-118.46806924816345 34.153353422410085,-118.4680081496888 34.15108281701051,-118.46919405507444 34.1487948604758,-118.46919507254036 34.148792313810176,-118.46919557402916 34.1487896176552,-118.46919743470674 34.14876798465156,-118.46920358199502 34.14874339549879,-118.46920395164064 34.14874116263671,-118.46921370008737 34.14862418127573,-118.46964222319643 34.14785682594087,-118.46964286803042 34.14785551487018,-118.46965606084392 34.14782473163871,-118.4765647719134 34.1455218,-118.5095215 34.1455218,-118.50952374257656 34.14552161922032,-118.50952592723709 34.14552108155005,-118.51776562723708 34.142774481550056,-118.51776856020305 34.14277310913142,-118.51777109940483 34.14277109958505,-118.5232619624869 34.137280336460435,-118.5397383272129 34.131788181558115,-118.53974126024642 34.13178680910462,-118.53974379949494 34.13178479949494,-118.54523466250315 34.12629393648674,-118.5534668 34.12354995725993,-118.56169893749686 34.12629393648674,-118.58916250050505 34.15375749949494,-118.58916503980778 34.15375950913812,-118.58916797290799 34.153760881598416,-118.59740767290799 34.156507381598416,-118.59740985749892 34.156507919232574,-118.5974121 34.1565081,-118.60839612811274 34.1565081,-118.61662713199796 34.159251767961734,-118.62211801841771 34.17572462714037,-118.62211939087528 34.175727560213886,-118.62212140050507 34.175730099494935,-118.62761185852654 34.18122055751641,-118.62898182510874 34.18670032408797,-118.62075033413201 34.19218805133938))"

    //val s = mutable.WrappedArray( Row("diner_id" = 19920023,"latitude" = 40.06823348, "longitude" = -75.07845307), Row("diner_id"=19920477,"latitude"=34.17946243, "longitude"= -118.5388946))

    val r1 = (19920023,40.06823348,-75.07845307)
    val r2 = (19920477,34.17946243,-118.5388946)
    val r3 = (64882733,38.861919400000000000,-86.482261660000000000)

   //val s = "[19920023 40.06823348 -75.07845307, 19920477 34.17946243 -118.5388946, 64882733 38.861919400000000000 -86.482261660000000000]"
    val s = "[45647897 40.11610031 -88.23402405, 28346532 40.08257675 -88.25135041, 15610897 40.12036514 -88.24258423, 15956558 40.09732437 -88.24887085, 22237201 40.11902999 -88.23291016, 48291917 40.09290695 -88.23718262, 47991314 40.11040496 -88.23565674, 46006074 40.11053466 -88.23428345, 45320836 40.13845825 -88.24571229, 2142001 40.14471817 -88.24463654]"

    PolygonUtils.pointInPolygon(PolygonStr, s) should not contain (19920477)
  }
}
