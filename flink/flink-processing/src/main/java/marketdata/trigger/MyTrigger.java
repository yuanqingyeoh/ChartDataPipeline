package marketdata.trigger;

import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class MyTrigger extends Trigger<Object, TimeWindow> {

    private MyTrigger() {}

    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            //ctx.registerEventTimeTimer(window.maxTimestamp());
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static MyTrigger create() {
        return new MyTrigger();
    }
}
