package com.pinvity.clustering.model;

import org.apache.log4j.Logger;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class Point implements Serializable {



    private final static double R = 6372.8; // In kilometers

    public double latitude, longitude;


    public Point() {}

    public Point(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Point add(Point other) {
        latitude += other.latitude;
        longitude += other.longitude;
        return this;
    }

    public Point div(long val) {
        latitude /= val;
        longitude /= val;

        return this;
    }

    public  double haversineDistance(Point other) {
        double dLat = Math.toRadians(other.latitude - this.latitude);
        double dLon = Math.toRadians(other.longitude- this.longitude);

        double a = Math.pow(Math.sin(dLat / 2),2) + Math.pow(Math.sin(dLon / 2),2) * Math.cos(this.latitude) * Math.cos(other.latitude);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }

//    public double haversineDistance(Point other) {
//        Double latDistance = toRad(other.latitude-this.latitude);
//        Double lonDistance = toRad(other.longitude-this.longitude);
//        Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
//                Math.cos(toRad(this.latitude)) * Math.cos(toRad(other.latitude)) *
//                        Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
//        Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
//
//        return R * c;
//    }

    private static Double toRad(Double value) {
        return value * Math.PI / 180;
    }

    public void clear() {
        latitude = longitude = 0.0;
    }

    @Override
    public String toString() {
        return "Latitude=[" + latitude + "] Longitude=[" + longitude + "]";
    }
}