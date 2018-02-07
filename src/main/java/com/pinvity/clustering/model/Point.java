package com.pinvity.clustering.model;

import org.apache.log4j.Logger;

import java.awt.geom.Point2D;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

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
    // http://www.aripd.com/blog/2013/11/27/java-calculation-of-geographic-midpoint.html
    public static Point2D.Double midpoint(List<Point2D.Double> points) {
        double Totweight = 0;
        double xt = 0;
        double yt = 0;
        double zt = 0;
        for (Point2D.Double point : points) {
            Double latitude = point.x;
            Double longitude = point.y;

            /**
             * Convert Lat and Lon from degrees to radians.
             */
            double latn = latitude * Math.PI / 180;
            double lonn = longitude * Math.PI / 180;

            /**
             * Convert lat/lon to Cartesian coordinates
             */
            double xn = Math.cos(latn) * Math.cos(lonn);
            double yn = Math.cos(latn) * Math.sin(lonn);
            double zn = Math.sin(latn);

            /**
             * Compute weight (by time) If locations are to be weighted equally,
             * set wn to 1
             */
            double years = 0;
            double months = 0;
            double days = 0;
            double wn = true ? 1 : (years * 365.25) + (months * 30.4375) + days;

            /**
             * Compute combined total weight for all locations.
             */
            Totweight = Totweight + wn;
            xt += xn * wn;
            yt += yn * wn;
            zt += zn * wn;
        }

        /**
         * Compute weighted average x, y and z coordinates.
         */
        double x = xt / Totweight;
        double y = yt / Totweight;
        double z = zt / Totweight;

        /**
         * If abs(x) < 10-9 and abs(y) < 10-9 and abs(z) < 10-9 then the
         * geographic midpoint is the center of the earth.
         */
        double lat = -0.001944;
        double lon = -78.455833;
        if (Math.abs(x) < Math.pow(10, -9) && Math.abs(y) < Math.pow(10, -9) && Math.abs(z) < Math.pow(10, -9)) {
        } else {

            /**
             * Convert average x, y, z coordinate to latitude and longitude.
             * Note that in Excel and possibly some other applications, the
             * parameters need to be reversed in the atan2 function, for
             * example, use atan2(X,Y) instead of atan2(Y,X).
             */
            lon = Math.atan2(y, x);
            double hyp = Math.sqrt(x * x + y * y);
            lat = Math.atan2(z, hyp);

            /**
             * Convert lat and lon to degrees.
             */
            lat = lat * 180 / Math.PI;
            lon = lon * 180 / Math.PI;
        }

        return new Point2D.Double(lat, lon);
    }

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