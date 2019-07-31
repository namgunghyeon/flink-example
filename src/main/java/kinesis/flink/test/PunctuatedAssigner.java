package kinesis.flink.test;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Movie> {
    private static final Logger LOG = LoggerFactory.getLogger(PunctuatedAssigner.class);

    @Override
    public long extractTimestamp(Movie element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Override
    public Watermark checkAndGetNextWatermark(Movie lastElement, long extractedTimestamp) {
        return null;
    }
}