package com.getindata.triggers

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.{OnMergeContext, TriggerContext}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

@SerialVersionUID(1L)
class WithEarlyTriggeringTrigger(interval: Long) extends Trigger[Object, TimeWindow] {

  private type JavaLong = java.lang.Long

  private val min: ReduceFunction[JavaLong] = new ReduceFunction[JavaLong] {
    override def reduce(value1: JavaLong, value2: JavaLong): JavaLong = Math.min(value1, value2)
  }

  private val serializer: TypeSerializer[JavaLong] = LongSerializer.INSTANCE.asInstanceOf[TypeSerializer[JavaLong]]

  private val stateDesc = new ReducingStateDescriptor[JavaLong]("fire-time", min, serializer)

  override def onElement(element: Object,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: TriggerContext): TriggerResult =
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      clearTimerForState(ctx)
      TriggerResult.FIRE
    }
    else {
      ctx.registerEventTimeTimer(window.maxTimestamp)

      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      if (fireTimestamp.get == null) {
        val start = timestamp - (timestamp % interval)
        val nextFireTimestamp = start + interval
        ctx.registerEventTimeTimer(nextFireTimestamp)
        fireTimestamp.add(nextFireTimestamp)
      }

      TriggerResult.CONTINUE
    }

  override def onProcessingTime(time: Long,
                                window: TimeWindow,
                                ctx: TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long,
                           window: TimeWindow,
                           ctx: TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp()) {
      clearTimerForState(ctx)
      TriggerResult.FIRE
    } else {
      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      if (fireTimestamp.get == time) {
        fireTimestamp.clear()
        fireTimestamp.add(time + interval)
        ctx.registerEventTimeTimer(time + interval)
        TriggerResult.FIRE
      } else {
        TriggerResult.CONTINUE
      }
    }
  }

  private def clearTimerForState(ctx: TriggerContext): Unit = {
    val timestamp = ctx.getPartitionedState(stateDesc).get()
    if (timestamp != null) {
      ctx.deleteEventTimeTimer(timestamp)
    }
  }


  override def canMerge: Boolean = true

  override def onMerge(window: TimeWindow,
                       ctx: OnMergeContext): TriggerResult = {
    ctx.mergePartitionedState(stateDesc)
    val nextFireTimestamp = ctx.getPartitionedState(stateDesc).get()
    if (nextFireTimestamp != null) {
      ctx.registerEventTimeTimer(nextFireTimestamp)
    }
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow,
                     ctx: TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp())
    val fireTimestamp = ctx.getPartitionedState(stateDesc)
    val timestamp = fireTimestamp.get
    if (timestamp != null) {
      ctx.deleteEventTimeTimer(timestamp)
      fireTimestamp.clear()
    }
  }

  override def toString: String = s"WithEarlyTriggeringTrigger($interval)"
}


object WithEarlyTriggeringTrigger {
  def triggerEvery(interval: Time) = new WithEarlyTriggeringTrigger(interval.toMilliseconds)
}
