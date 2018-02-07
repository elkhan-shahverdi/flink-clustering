package com.pinvity.clustering.repository;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.pinvity.clustering.model.Centroid;
import com.pinvity.clustering.model.Point;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DBHandlerMongo {

    private final static Logger logger        = Logger.getLogger(DBHandlerMongo.class);
    private static DBHandlerMongo ourInstance = new DBHandlerMongo();


    public static DBHandlerMongo getInstance() {
        if (ourInstance == null)
            return new DBHandlerMongo();
        return ourInstance;
    }

    MongoClient mongoClient;
    MongoDatabase pinvity;


    private DBHandlerMongo() {
        MongoClientURI uri  = new MongoClientURI("mongodb://mongouser:someothersecret@138.197.171.240:27017/pinvity");
        mongoClient         = new MongoClient(uri);
        pinvity             = mongoClient.getDatabase("pinvity");
    }


    public List<Point> getActivities(int limit) {

        FindIterable<Document> mydatabaserecords;

        if (limit > 0)
            mydatabaserecords = pinvity
                    .getCollection("activities")
                    .find(new BsonDocument("status", new BsonInt32(0))).limit(limit)
                    .projection(new BsonDocument("location.LongLat", new BsonInt32(1)));
        else
            mydatabaserecords = pinvity
                    .getCollection("activities")
                    .find(new BsonDocument("status", new BsonInt32(0)))
                    .projection(new BsonDocument("location.LongLat", new BsonInt32(1)));

        MongoCursor<Document> iterator = mydatabaserecords.iterator();
        List<Point> points = new LinkedList<>();
        int unhandledPointCount = 0;
        while (iterator.hasNext()) {
            Document doc = iterator.next();
            ArrayList<Double> point = (ArrayList<Double>) ((Document) doc.get("location")).get("LongLat");
            if (point.size() == 2)
                try {
                    points.add(new Point(point.get(1), point.get(0)));
                }catch (ClassCastException ex){
                    logger.error(ex.getMessage(),ex.getCause());
                    logger.info(point);
                    unhandledPointCount++;
                }
        }

        logger.info("#unhandledPointCount "+unhandledPointCount);
        return points;
    }



    public void insertClusters(List<Centroid> centroids,int k){
        LocalDateTime startTimeInsert = LocalDateTime.now();
        pinvity.createCollection("cluster"+k+"_temp");
        List<Document> docs = new ArrayList<>();
        centroids.forEach(centroid -> {
            Document temp = new Document();
            temp.put("clusterId",centroid.id);
            temp.put("pinType",centroid.count);
            temp.put("longitude", centroid.longitude);
            temp.put("latitude",centroid.latitude);
            docs.add(temp);
        });

        pinvity.getCollection("cluster"+k+"_temp").insertMany(docs);
        LocalDateTime endTimeInsert = LocalDateTime.now();
        logger.info("INSERT of collection take => "  +startTimeInsert.until(endTimeInsert, ChronoUnit.MILLIS)+" millis");
        LocalDateTime startTime = LocalDateTime.now();
        if(pinvity.getCollection("cluster"+k) != null)
            pinvity.getCollection("cluster"+k).drop();

        MongoNamespace newName = new MongoNamespace("pinvity" ,"cluster"+k);
        if(pinvity.getCollection("cluster"+k+"_temp") != null)
            pinvity.getCollection("cluster"+k+"_temp").renameCollection(newName);

        LocalDateTime endTime = LocalDateTime.now();
        logger.info("RENAME of collection take => "  +startTime.until(endTime, ChronoUnit.MILLIS)+" millis");
    }


    public void insertClusters(Centroid centroid,int k){
        pinvity.createCollection("cluster"+k);

        Document temp = new Document();
        temp.put("clusterId",centroid.id);
        temp.put("pinType",centroid.count);
        temp.put("longitude", centroid.longitude);
        temp.put("latitude",centroid.latitude);
        UpdateOptions uo = new UpdateOptions();
        uo.upsert(true);
        pinvity.getCollection("cluster"+k).updateOne(Filters.eq("clusterId",centroid.id),temp,uo);

    }


}
