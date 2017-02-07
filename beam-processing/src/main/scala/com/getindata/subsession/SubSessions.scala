package com.getindata.subsession

import com.getindata.UserEvent

import scala.annotation.tailrec
import scala.collection.Iterable
import scala.collection.JavaConverters._

object SubSessions {
  def mapToSubSession(l: java.lang.Iterable[UserEvent],
                      splitPredicate: java.util.function.Predicate[UserEvent],
                      produceSubsession: SubSessionProcessor
                     ): Unit = {
    internalMapToSubSession(l.asScala, splitPredicate, produceSubsession)
  }

  @tailrec private def internalMapToSubSession(l: Iterable[UserEvent],
                                               splitPredicate: java.util.function.Predicate[UserEvent],
                                               produceSubsession: SubSessionProcessor): Unit = {
    l.span(!splitPredicate.test(_)) match {
      case (Nil, Nil) =>
      case (x, Nil) => produceSubsession.processSubSession(x.toList.asJava)
      case (Nil, (y :: ys)) =>
        internalMapToSubSession(ys, splitPredicate, produceSubsession)
      case (x, (y :: ys)) =>
        produceSubsession.processSubSession(x.toList.asJava)
        internalMapToSubSession(ys, splitPredicate, produceSubsession)
    }
  }
}
