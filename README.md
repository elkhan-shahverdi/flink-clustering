# Flink-clustering 


This is the locations data clustering solution. Based on [Apache Flink Examples](https://github.com/apache/flink/tree/master/flink-examples) the project created for understanding the big streaming concept and providing the faster solution. Problem solved with Apache Flink stream processing framework and K-Means algorithm.  

The distance between location point calculated with haversine formula.
  
The solution gets all locations from MongoDB. Depending locations count deciding to create  &rightarrow; n thread for clustering locations with different K values.  
Every thread starts to cluster the same collection with different K values. Threads K values could be  8 or it folds. After calculations clusters center storing in MongoDB collations.