package com.renfer.changestreams;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.time.StopWatch;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.stereotype.Repository;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.FullDocument;

@Repository 
@Scope("prototype")
public class MongoDBWatch implements Callable<String> {

    private static Logger LOG = LoggerFactory.getLogger(MongoDBWatch.class);

    private MongoCollection<Document> sourceCollection;
    private MongoCollection<Document> resumeTokensCollection;
    private final StopWatch stopWatch;
    
    @Value("${mongodb.changestream.partitionIndex}")
    private Integer partitionIndex;
  
    @Value("${mongodb.changestream.totalPartitionCount}")
    private Integer totalPartitionCount;

    @Value("${mongodb.changestream.startAt}")
    private String startAt;
    
    @Value("${mongodb.changestream.batchSize}")
    private Integer batchSize;
    
    @Value("${mongodb.changestream.db}")
    private String db;
    
    @Value("${mongodb.changestream.collection}")
    private String collection;
    
    @Value("${mongodb.changestream.resumTokenCollection}")
    private String resumTokenCollection;

	private MongoDatabaseFactory mongo;
    
    public void setPartitionIndex(int partitionIndex) {
    	this.partitionIndex  = partitionIndex;
    }
    
    public Integer getPartitionIndex() {
    	return this.partitionIndex;
    }
    
    public void setTotalPartitionCount(int totalPartitionCount) {
    	this.totalPartitionCount = totalPartitionCount;
    }
    
    @Autowired
    public MongoDBWatch(MongoDatabaseFactory mongo) {
        this.mongo = mongo;
        this.stopWatch = new StopWatch();
    }
    
    @Override
	public String call() throws ParseException {
        this.sourceCollection = mongo.getMongoDatabase(this.db).getCollection(this.collection);
        this.resumeTokensCollection = mongo.getMongoDatabase(this.db).getCollection(this.resumTokenCollection);
    	    	
        Document tokenFilter = new Document("namespace", new Document("db", "asml").append("coll", "parts"));
        
        ChangeStreamIterable<Document> changeStreamDocuments;
        if(this.startAt.equals("ResumeToken")) {
        	Document resumeToken = this.resumeTokensCollection.find(tokenFilter).first();
        	if(resumeToken != null) {
        		LOG.info("Resume after resume token: "+resumeToken);
        		changeStreamDocuments = this.sourceCollection.watch(splitStreamPipeline(this.totalPartitionCount, this.partitionIndex)).batchSize(this.batchSize).resumeAfter(resumeToken.toBsonDocument()).fullDocument(FullDocument.UPDATE_LOOKUP);
        	} else {
        		LOG.info("Atart on the next change");
        		changeStreamDocuments = this.sourceCollection.watch(splitStreamPipeline(this.totalPartitionCount, this.partitionIndex)).batchSize(this.batchSize).fullDocument(FullDocument.UPDATE_LOOKUP);
        	}
        } else {
        	BsonTimestamp ts = bsonTimestampFrom(this.startAt);
            LOG.info("Start at operation time: "+Instant.ofEpochSecond(ts.getTime()).toString());
            changeStreamDocuments = this.sourceCollection.watch(splitStreamPipeline(this.totalPartitionCount, this.partitionIndex)).batchSize(this.batchSize).startAtOperationTime(ts).fullDocument(FullDocument.UPDATE_LOOKUP);
        }

        this.stopWatch.start();
        AtomicInteger counter = new AtomicInteger();
        long timeElapsed = 0;

        changeStreamDocuments.forEach((doc) -> {
        	var oid = doc.getFullDocument().getObjectId("_id");
        	LOG.debug("OID of doc: "+oid);
            int localCounter = counter.incrementAndGet();
            if(localCounter%this.batchSize == 0 && localCounter > 0) {
                BsonDocument docResumeToken = doc.getResumeToken();
                docResumeToken.append("namespace", doc.getNamespaceDocument());
                docResumeToken.append("utc_timestamp", new BsonDateTime(System.currentTimeMillis()));
                this.resumeTokensCollection.updateOne(tokenFilter, new Document("$set", docResumeToken), new UpdateOptions().upsert(true));

                this.stopWatch.split();
                var localEllapsedSeconds = (timeElapsed + this.stopWatch.getSplitTime()) / 1000;
                double docsProcessedPerSec = counter.get() / localEllapsedSeconds;
                LOG.info("(Stream "+this.partitionIndex+") documents processed per second: "+ docsProcessedPerSec+
                        ", total docs processed: " + counter + ", total time elapsed (sec): " + localEllapsedSeconds);
                
            }
        });
        LOG.info("watch stopped...");
        return "watch stopped...";
    }

    private List<Bson> splitStreamPipeline(int n, int m){

        var modValueArray = new BsonArray();
        modValueArray.add(new Document("$toHashedIndexKey", "$documentKey._id").toBsonDocument());
        modValueArray.add(new BsonInt32(n));

        var eqValueArray = new BsonArray();
        eqValueArray.add(new Document("$abs",
                new Document("$mod",
                        modValueArray
                )
        ).toBsonDocument());
        eqValueArray.add(new BsonInt32(m));

        Bson match = Aggregates.match(new Document("$expr",
                new Document("$eq",
                        eqValueArray
                )
        ));

        return Arrays.asList(match);
    }
    
    private BsonTimestamp bsonTimestampFrom(String startAt) throws ParseException {
        DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        Date date = df.parse(startAt);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return new BsonTimestamp((int) cal.getTime().toInstant().getEpochSecond(),0);
    }
}




