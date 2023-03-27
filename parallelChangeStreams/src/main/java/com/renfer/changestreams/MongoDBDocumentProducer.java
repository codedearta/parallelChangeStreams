package com.renfer.changestreams;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.LinkedList;
import java.util.Random;

@Repository
public class MongoDBDocumentProducer {

    private final MongoCollection<Document> collection;

    @Autowired
    public MongoDBDocumentProducer(MongoDatabaseFactory mongo) {
        this.collection = mongo.getMongoDatabase("asml").getCollection("parts2");
    }

    public void run(int numberOfBatches){
        Random random = new Random(new Date().getTime());
        for(int i=0;i<numberOfBatches;i++){
            LinkedList<Document> l = new LinkedList<>();
            for(int j=0;j<100;j++) {
                l.add(new Document("name", "Bob " + random.nextInt()));
                l.add(new Document("name", "John " + random.nextInt()));
                l.add(new Document("name", "Joe " + random.nextInt()));
                l.add(new Document("name", "Pete " + random.nextInt()));
                l.add(new Document("name", "Randy " + random.nextInt()));
                l.add(new Document("name", "Rich " + random.nextInt()));
                l.add(new Document("name", "Nyla " + random.nextInt()));
                l.add(new Document("name", "Anna " + random.nextInt()));
                l.add(new Document("name", "Rachel " + random.nextInt()));
                l.add(new Document("name", "Alice " + random.nextInt()));
            }
            collection.insertMany(l);
        }
    }
}
