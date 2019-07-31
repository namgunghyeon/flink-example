package kinesis.flink.test;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;


public class MovieSchema  extends AbstractDeserializationSchema<Movie> {
    private static final Logger LOG = LoggerFactory.getLogger(MovieSchema.class);

    @Override
    public Movie deserialize(byte[] bytes) {
        try {

            Movie event = Movie.parseEvent(bytes);

            return event;
        } catch (Exception e) {
            LOG.debug("cannot parse event '{}'", new String(bytes, StandardCharsets.UTF_8), e);

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Movie movie) {
        return false;
    }

    @Override
    public TypeInformation<Movie> getProducedType() {
        return TypeExtractor.getForClass(Movie.class);
    }

}
