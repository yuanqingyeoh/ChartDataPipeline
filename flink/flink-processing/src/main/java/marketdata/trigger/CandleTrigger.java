package marketdata.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class CandleTrigger extends Trigger<Object, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(CandleTrigger.class);
    private final Duration candleOffest;

    private CandleTrigger(Duration candleOffest) {
        this.candleOffest = candleOffest;
    }

    @Override
    public TriggerResult onElement(
            Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {

        LocalDateTime windowStart =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getStart()),
                        TimeZone.getDefault().toZoneId());

        LocalDateTime windowEnd =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(window.getEnd()),
                        TimeZone.getDefault().toZoneId());
        LocalDateTime currentWatermark = LocalDateTime.ofInstant(Instant.ofEpochMilli(ctx.getCurrentWatermark()),
                TimeZone.getDefault().toZoneId());
        LocalDateTime triggerTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(window.maxTimestamp() - candleOffest.toMillis()),
                TimeZone.getDefault().toZoneId());
        LocalDateTime eventTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                TimeZone.getDefault().toZoneId());
        LOG.info("Processing: "+ windowStart + " to " + windowEnd +" Current watermark: " + currentWatermark + " Trigger Timestamp: " + triggerTimestamp + " " +eventTime);
        if (window.maxTimestamp() - candleOffest.toMillis() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
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
        return TriggerResult.CONTINUE;
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
        return "CandleTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static CandleTrigger create(Duration candleOffest) {
        return new CandleTrigger(candleOffest);
    }
}
