package kmeans;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
	
	public List<Point> points;
	public Point centroid;
	public int id;
	
	//Creates a new Cluster
	public Cluster(int id) {
		this.id = id;
		this.points = new ArrayList<Point> ();
		this.centroid = null;
	}

	public List<Point>  getPoints() {
		return points;
	}
	
	public void addPoint(Point point) {
		this.points.add(point);
	}

	public void setPoints(List<Point> points) {
		this.points = points;
	}

	public Point getCentroid() {
		return centroid;
	}

	public void setCentroid(Point centroid) {
		List<Double> dimensions = new ArrayList<Double>(centroid.getdimensions().size());
		for (int k = 0; k < centroid.getdimensions().size(); k++) {
			dimensions.add((double) centroid.getdimensions().get(k));
		}
		PexilLocation pexilLocation = new PexilLocation(centroid.getPexilLocation().getI(), centroid.getPexilLocation().getJ());
		centroid = new Point(pexilLocation, dimensions, this.id);
		
		this.centroid = centroid;
	}

	public int getId() {
		return id;
	}
	
	public void clear() {
		this.points.clear();
	}
	
	public void plotCluster() {
		System.out.println("[Cluster: " + id+"]");
		System.out.println("[Centroid: " + centroid + "]");
		System.out.println("[Points: \n");
		for(Point p : this.points) {
			System.out.println(p);
		}
		System.out.println("]");
	}

}