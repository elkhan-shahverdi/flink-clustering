package com.pinvity.clustering;

import com.pinvity.clustering.model.Centroid;
import com.pinvity.clustering.model.Point;
import com.pinvity.clustering.repository.DBHandlerMongo;

import org.apache.log4j.Logger;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@SuppressWarnings("serial")
public class Main {

    private  final static Logger logger = Logger.getLogger(Main.class);
    private final static int initialCluster = 8;

    public static void main(String[] args){
        List<Point> points = DBHandlerMongo.getInstance().getActivities(0);
        int k = determineK(1,points.size());


        final CountDownLatch countDownLatch     = new CountDownLatch(k);
        ExecutorService executorService         = Executors.newFixedThreadPool(k);


        logger.info("NEW FIXED EXECUTORS SERVICE STARTED WITH SIZE: "+k+" DATA SIZE: "+points.size());
        for (int i = 1; i <= k; i++){

            List<Centroid> centroidDataSet = getCentroidDataSet(pow(initialCluster,i),points);
            KMeans kMeans = new KMeans(pow(initialCluster,i),points,centroidDataSet,countDownLatch);

            logger.info("NEW THREAD WITH K=["+pow(initialCluster,i)+"] STARTED");
            executorService.submit(kMeans);
        }

        // wait until latch counted down to 0
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(),e.getCause());
        } finally {
            executorService.shutdown();
            logger.info("MAIN Thread Duration: "+ KMeans.totalDuration/60 + " minutes");
        }
    }

    private static List<Centroid> getCentroidDataSet(int k,List<Point> points) {
        List<Centroid> centroids = new LinkedList<>();
        Random rand = new Random();
        int size = points.size();
        for (int i = 0; i < k; i++) {
            int randIndex = rand.nextInt(size);
            centroids.add(new Centroid(i + 1, points.get(randIndex).latitude, points.get(randIndex).longitude));
        }
        return centroids;
    }

    private static int determineK(int deep, int size){
        int newSize = size/8;
        if(newSize > 200){
            deep++;
            return determineK(deep,newSize);
        }
        return deep;
    }

    private static int pow(int number, int pow){
        int result = 1;
        while (pow != 0 ){
            result = result * 8;
            pow--;
        }
        return result;
    }
}
