package kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.cloudwatch.model.Dimension;

public class Point {

	private List<Double> dimensions;
	private PexilLocation pexilLocation;
	private int cluster_number = 0;

	public Point(PexilLocation pexilLocation, List<Double> dimensions, int cluster_number) {
		this.setPexilLocation(pexilLocation);
		this.setdimensions(dimensions);
		this.cluster_number = cluster_number;
	}

	// public int getNumdimensions() {
	// return dimensions.size();
	// }

	public PexilLocation getPexilLocation() {
		return pexilLocation;
	}

	public void setPexilLocation(PexilLocation pexilLocation) {
		this.pexilLocation = new PexilLocation(pexilLocation.getI(), pexilLocation.getJ());
	}

	public List<Double> getdimensions() {
		// List<Double> dimensions = new ArrayList<Double>();
		// for (int i = 0; i < this.dimensions.size(); i++) {
		// dimensions.add(this.dimensions.get(i));
		// }
		return dimensions;
	}

	public void setdimensions(List<Double> dimensions) {
		this.dimensions = new ArrayList<Double>(dimensions.size());
		for (int i = 0; i < dimensions.size(); i++) {
			this.dimensions.add(dimensions.get(i));
		}
	}

	public void setCluster(int cluster_number) {
		this.cluster_number = cluster_number;
	}

	public int getCluster() {
		return cluster_number;
	}

	// Calculates the distance between two points.
	protected static double distance(Point p, Point centroid) {
		double squares = 0;
		for (int i = 0; i < p.getdimensions().size(); i++) {
			squares += Math.pow((centroid.getdimensions().get(i) - p.getdimensions().get(i)), 2);
		}
		return Math.sqrt(squares);
	}

	public String toString() {
		String s = "(";
		for (int i = 0; i < this.dimensions.size() - 1; i++) {
			s += this.dimensions.get(i) + ",";
		}
		if (this.dimensions.size() > 0) {
			s += this.dimensions.get(this.dimensions.size() - 1) + ", c=" + cluster_number + ", loc = ("
					+ pexilLocation.getI() + "," + pexilLocation.getJ() + ") " + ")";
		} else {
			s += ", c=" + cluster_number + ", loc = ("
					+ pexilLocation.getI() + "," + pexilLocation.getJ() + ") " + ")";
		}
		return s;
	}
}