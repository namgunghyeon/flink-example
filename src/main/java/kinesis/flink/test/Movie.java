package kinesis.flink.test;

import com.esotericsoftware.minlog.Log;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;

public abstract class Movie {
    private static final String TYPE_FIELD = "type";

    private static final Logger LOG = LoggerFactory.getLogger(Movie.class);

    private static final Gson gson = new GsonBuilder()
            .create();

    public static Movie parseEvent(byte[] movie) {
        //parse the event payload and remove the type attribute
        JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(movie)));
        JsonElement jsonElement = Streams.parse(jsonReader);
        JsonElement labelJsonElement = jsonElement.getAsJsonObject();

        if (labelJsonElement == null) {
            throw new IllegalArgumentException("Event does not define a type field: " + new String(movie));
        }

        //LOG.info("parse event {}", jsonElement.getAsJsonObject().toString());

        return gson.fromJson(jsonElement, MovieData.class);

    }

    /**
     * @return timestamp in epoch millies
     */
    public abstract long getTimestamp();
}
