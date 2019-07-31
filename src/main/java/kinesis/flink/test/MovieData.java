package kinesis.flink.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class MovieData extends Movie {
    public final String movieId;
    public final String userId;
    public final Long timestamp;
    public final Instant dropoffDatetime;


    private static final Logger LOG = LoggerFactory.getLogger(MovieData.class);

    public MovieData() {
        movieId = null;
        userId = null;
        timestamp = null;
        dropoffDatetime = Instant.EPOCH;
    }

    public String getMoveId() {
        return movieId;
    }

    public String getUsrId() {
        return userId;
    }

    public Long getTime() {
        return timestamp;
    }

    @Override
    public long getTimestamp() {
        return dropoffDatetime.toEpochMilli();
    }
}
