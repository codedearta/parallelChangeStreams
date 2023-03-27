package com.renfer.changestreams;

import com.mongodb.client.model.Aggregates;
import org.bson.*;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class NonSpringTests {


    @Test
    public void bsonTimeStamp(){
        int epochSecond = (int)new GregorianCalendar(2023, Calendar.MARCH, 13, 6, 0).getTime().toInstant().getEpochSecond();
        System.out.println(epochSecond);
        var ts = new BsonTimestamp(epochSecond,0);
        System.out.println(ts);
        System.out.println(Instant.ofEpochSecond(ts.getTime()).toString());
    }

    @Test
    public void streamSplitter(){
        int n = 1;
        var m = new BsonArray();
        for(int i=0;i<n;i++){
            m.add(new BsonInt32(i));
        }

        var modValueArray = new BsonArray();
        modValueArray.add(new Document("$toHashedIndexKey", "$documentKey._id").toBsonDocument());
        modValueArray.add(new BsonInt32(n));

        var eqValueArray = new BsonArray();
        eqValueArray.add(new Document("$abs",
                new Document("$mod",
                        modValueArray
                )
        ).toBsonDocument());
        eqValueArray.add(m);

        Bson match = Aggregates.match(new Document("$expr",
                new Document("$eq",
                        eqValueArray
                )
        ));

        var pipeline = new BsonArray();
        pipeline.add(match.toBsonDocument());
        System.out.println(pipeline.asArray());
    }

}
