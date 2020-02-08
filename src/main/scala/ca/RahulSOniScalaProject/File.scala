package ca.RahulSOniScalaProject

import scala.io.{BufferedSource, Source}

object File extends App {

  val tripList: List[Trip] = Source
    .fromFile("/Users/admin/Downloads/gtfs_stm/trips.txt")
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Trip(p(0).toInt, p(1), p(2), p(3), p(4).toInt, p(5).toInt, p(6).toInt,
      if (p(7).isEmpty) None else Some(p(7)),
      if (p(8).isEmpty) None else Some(p(8))))


  val routeList: List[Route] = Source
    .fromFile("/Users/admin/Downloads/gtfs_stm/routes.txt")
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Route(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6),p(7)))


  val routeTrips: List[JoinOutput] = new GenericNestedLoopJoin[Trip, Route]((i, j) => i.route_id == j.route_id)
    .join(tripList, routeList)

  routeTrips
    .map( joinOutput => Trip.toCsv(joinOutput.left.asInstanceOf[Trip]))
    .foreach(println)

}
