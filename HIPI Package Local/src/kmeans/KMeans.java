/* 
 * KMeans.java ; Cluster.java ; Point.java
 *
 * Solution implemented by DataOnFocus
 * www.dataonfocus.com
 * 2015
 *
*/
package kmeans;

import java.util.ArrayList;
import java.util.List;

import ProcessWithHIPI.FloatImageContainer;
import hipi.image.FloatImage;

public class KMeans {

	private int NUM_CLUSTERS;
	private int NUM_POINTS;
	private int NUM_DIMENSIONS;

	private int width;
	private int height;

	private List<Point> points;
	private List<Cluster> clusters;

	public KMeans() {
		this.points = new ArrayList<Point>();
		this.clusters = new ArrayList<Cluster>();
	}

	// Initializes the process
	public void init(FloatImageContainer floatImageContainer, int NUM_CLUSTERS) {
		this.NUM_CLUSTERS = NUM_CLUSTERS;

		// Create Points
		FloatImage floatImage = floatImageContainer.getFloatImage();
		this.NUM_DIMENSIONS = floatImage.getBands();
		this.width = floatImage.getWidth();
		this.height = floatImage.getHeight();

		for (int i = 0; i < floatImage.getHeight(); i++) {
			for (int j = 0; j < floatImage.getWidth(); j++) {
				List<Double> dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
				for (int k = 0; k < floatImage.getBands(); k++) {
					dimensions.add((double) floatImage.getPixel(j, i, k));
				}
				PexilLocation pexilLocation = new PexilLocation(i, j);
				Point point = new Point(pexilLocation, dimensions, 0);
				this.points.add(point);
			}
		}

		this.NUM_POINTS = floatImage.getHeight() * floatImage.getWidth();

		// Create Clusters
		// Set Random Centroids
		
//		for (int i = 0; i < NUM_CLUSTERS; i++) {
//			Cluster cluster = new Cluster(i);
//			int x = (int) (Math.random() * floatImage.getWidth());
//			int y = (int) (Math.random() * floatImage.getHeight());
//			List<Double> dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//			for (int k = 0; k < floatImage.getBands(); k++) {
//				dimensions.add((double) floatImage.getPixel(x, y, k));
//			}
//			PexilLocation pexilLocation = new PexilLocation(-1, -1);
//			Point centroid = new Point(pexilLocation, dimensions, 0);
//			cluster.setCentroid(centroid);
//			this.clusters.add(cluster);
//		}
		
		Cluster cluster;
		int index=0;
		for (int i = 0; i < this.NUM_CLUSTERS; i++) {
			if (i==0) {
				index = 0;//top left
			}else if (i==1) {
				index = this.width-1;//top right
			}else if (i==2) {
				index = (int)((this.height*this.width)/2)-1;//center
			}else if (i==3) {
				index = this.height*(this.width-1);//bottom left
			}else if (i==4) {
				index = (this.height*this.width)-1;//bottom right
			}
			cluster = new Cluster(i);
			cluster.setCentroid(this.points.get(index));
			this.clusters.add(cluster);
		}
		
//		Cluster cluster = new Cluster(0);
//		cluster.setCentroid(this.points.get(0));//top left
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(1);
//		cluster.setCentroid(this.points.get(this.width-1));//top right
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(2);
//		cluster.setCentroid(this.points.get((int)((this.height*this.width)/2)-1));//center
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(3);
//		cluster.setCentroid(this.points.get(this.height*(this.width-1)));//bottom left
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(4);
//		cluster.setCentroid(this.points.get((this.height*this.width)-1));//bottom right
//		this.clusters.add(cluster);
		
		
//		Cluster cluster = new Cluster(0);
//		List<Double> dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//		dimensions.add(0.0);
//		dimensions.add(0.0);
//		dimensions.add(0.0);
//		cluster.setCentroid(new Point(new PexilLocation(0, 0), dimensions , -1));
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(1);
//		dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//		dimensions.add(0.0);
//		dimensions.add(51.0);
//		dimensions.add(0.0);
//		cluster.setCentroid(new Point(new PexilLocation(0, 0), dimensions , -1));
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(2);
//		dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//		dimensions.add(0.0);
//		dimensions.add(102.0);
//		dimensions.add(0.0);
//		cluster.setCentroid(new Point(new PexilLocation(0, 0), dimensions , -1));
//		this.clusters.add(cluster);
//		
//		cluster = new Cluster(3);
//		dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//		dimensions.add(0.0);
//		dimensions.add(153.0);
//		dimensions.add(0.0);
//		cluster.setCentroid(new Point(new PexilLocation(0, 0), dimensions , -1));
//		this.clusters.add(cluster);
//
//		cluster = new Cluster(4);
//		dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
//		dimensions.add(0.0);
//		dimensions.add(204.0);
//		dimensions.add(0.0);
//		cluster.setCentroid(new Point(new PexilLocation(0, 0), dimensions , -1));
//		this.clusters.add(cluster);
		
		// Print Initial state
		// plotClusters();
	}

	private void plotClusters() {
		for (int i = 0; i < this.NUM_CLUSTERS; i++) {
			Cluster c = this.clusters.get(i);
			c.plotCluster();
		}
	}

	// The process to calculate the K Means, with iterating method.
	public Segments calculate() {
		boolean finish = false;
		int iteration = 0;

		// Add in new data, one at a time, recalculating centroids with each new
		// one.
		while (!finish) {
			// Clear cluster state
			clearClusters();

			List<Point> lastCentroids = getCentroids();

			// Assign points to the closer cluster
			assignCluster();

			// Calculate new centroids.
			calculateCentroids();

			iteration++;

			List<Point> currentCentroids = getCentroids();

			// Calculates total distance between new and old Centroids
			double distance = 0;
			for (int i = 0; i < lastCentroids.size(); i++) {
				distance += Point.distance(lastCentroids.get(i), currentCentroids.get(i));
			}
			// System.out.println("#################");
			// System.out.println("Iteration: " + iteration);
			// System.out.println("Centroid distances: " + distance);
			// plotClusters();

			System.out.println("iteration = " + iteration);
			
			if (distance == 0) {
				finish = true;
			}
		}
		//plotClusters();
		return new Segments(clusters, this.width, this.height, this.NUM_DIMENSIONS);
	}

	private void clearClusters() {
		for (Cluster cluster : clusters) {
			cluster.clear();
		}
	}

	private List<Point> getCentroids() {
		List<Point> centroids = new ArrayList<Point>(this.NUM_CLUSTERS);
		for (Cluster cluster : this.clusters) {
			Point centroid = cluster.getCentroid();
			Point point = new Point(centroid.getPexilLocation(), centroid.getdimensions(), 0);
			centroids.add(point);
		}
		return centroids;
	}

	private void assignCluster() {
		int cluster = 0;
		double distance = 0.0;

		for (Point point : points) {
			double min = Double.MAX_VALUE;
			for (int i = 0; i < this.NUM_CLUSTERS; i++) {
				Cluster c = this.clusters.get(i);
				distance = Point.distance(point, c.getCentroid());
				if (distance < min) {
					min = distance;
					cluster = i;
				}
			}
			point.setCluster(cluster);
			this.clusters.get(cluster).addPoint(point);
		}
	}

	private void calculateCentroids() {
		for (Cluster cluster : this.clusters) {
			List<Point> list = cluster.getPoints();

			List<Double> dimensions = new ArrayList<Double>(this.NUM_DIMENSIONS);
			for (int i = 0; i < this.NUM_DIMENSIONS; i++) {
				dimensions.add(0.0);
			}

			for (Point point : list) {
				for (int i = 0; i < this.NUM_DIMENSIONS; i++) {
					dimensions.set(i, dimensions.get(i) + point.getdimensions().get(i));
				}
			}

			Point centroid = cluster.getCentroid();

			if (list.size() != 0) {
				for (int i = 0; i < this.NUM_DIMENSIONS; i++) {
					dimensions.set(i, dimensions.get(i) / list.size());
				}
				centroid.setdimensions(dimensions);
			}

		}
	}
}