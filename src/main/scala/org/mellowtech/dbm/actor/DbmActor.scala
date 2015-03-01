package org.mellowtech.dbm.actor

import akka.actor.Actor
import akka.actor._
import scala.concurrent.duration._
import org.mellowtech.dbm.DbIndexer

case object ScheduleRefresh
case class NewDelay(delay: Int)

class SearchRefresh(val initialDelay: Int, delay: Int, val dbi: DbIndexer[_]) extends Actor with ActorLogging{
  
  import context._
  import scala.util.{Try,Success,Failure}
  var _delay = delay
  
   override def preStart() =
    system.scheduler.scheduleOnce(initialDelay millis, self, ScheduleRefresh)
 
  // override postRestart so we don't call preStart and schedule a new message
  override def postRestart(reason: Throwable) = {}
 
  def receive = {
    case ScheduleRefresh =>
      // send another periodic tick after the specified delay
      //searchManager.maybeRefresh
      dbi.refresh match {
        case Success(_) => system.scheduler.scheduleOnce(_delay millis, self, ScheduleRefresh)
        case Failure(f) => log.warning("abort because of error: "+f)
      }
    case NewDelay(delay) => _delay = delay
  }
  
}

object SearchRefresh {
  
  def props(initDelay: Int, delay: Int, dbi: DbIndexer[_]):Props = Props(new SearchRefresh(initDelay, delay, dbi))
  
}