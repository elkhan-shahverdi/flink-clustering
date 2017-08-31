package com.pinvity.clustering.model;

public class Centroid extends Point {

    public int id;
    public long count;

    public Centroid() {}

    public Centroid(int id, double latitude, double longitude) {
        super(latitude, longitude);
        this.id = id;
    }

    public Centroid(int id, Point p) {
        super(p.latitude, p.longitude);
        this.id = id;
    }


    public Centroid(int id, Point p, long count) {
        super(p.latitude, p.longitude);
        this.id = id;
        this.count = count;
    }

    @Override
    public String toString() {
        return "#" + id + " count=[" + count + "]" + super.toString();
    }
}