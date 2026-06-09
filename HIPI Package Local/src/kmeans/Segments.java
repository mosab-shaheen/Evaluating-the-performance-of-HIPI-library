package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import hipi.util.ByteUtils;

public class Segments implements Writable, RawComparator<BinaryComparable> {
	private int width;
	private int height;
	private int bands;
	private List<Cluster> clusters;

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public int getBands() {
		return bands;
	}

	public void setBands(int bands) {
		this.bands = bands;
	}

	public List<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(List<Cluster> clusters) {
		this.clusters = new ArrayList<Cluster>(clusters.size());
		for (int i = 0; i < clusters.size(); i++) {
			this.clusters.add(clusters.get(i));
		}
	}

	public Segments() {
		this.width = 0;
		this.height = 0;
		this.bands = 0;
		setClusters(new ArrayList<Cluster>());
	}

	public Segments(List<Cluster> clusters, int width, int height, int bands) {
		this.width = width;
		this.height = height;
		this.bands = bands;
		setClusters(clusters);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.width);
		out.writeInt(this.height);
		out.writeInt(this.bands);
		out.writeInt(this.clusters.size());
		for (Cluster cluster : this.clusters) {
			out.writeInt(cluster.getPoints().size());
			for (Point point : cluster.getPoints()) {
				out.writeInt(point.getPexilLocation().getI());
				out.writeInt(point.getPexilLocation().getJ());
				out.writeInt(point.getdimensions().size());
				for (Double d : point.getdimensions()) {
					out.writeDouble(d);
				}
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.width = in.readInt();
		this.height = in.readInt();
		this.bands = in.readInt();
		int numClusters = in.readInt();
		this.clusters = new ArrayList<Cluster>(numClusters);
		for (int i = 0; i < numClusters; i++) {
			Cluster cluster = new Cluster(i);
			int numPoints = in.readInt();
			for (int j = 0; j < numPoints; j++) {
				int x = in.readInt();
				int y = in.readInt();
				PexilLocation pexilLocation = new PexilLocation(x, y);

				int numDimensions = in.readInt();
				List<Double> dimensions = new ArrayList<Double>(numDimensions);
				for (int k = 0; k < numDimensions; k++) {
					Double d = in.readDouble();
					dimensions.add(d);
				}

				Point point = new Point(pexilLocation, dimensions, i);
				cluster.addPoint(point);
			}
			this.clusters.add(cluster);
		}
	}

	@Override
	public int compare(BinaryComparable o1, BinaryComparable o2) {
		byte[] b1 = o1.getBytes();
		byte[] b2 = o2.getBytes();
		int length1 = o1.getLength();
		int length2 = o2.getLength();

		return compare(b1, 0, length1, b2, 0, length2);
	}

	@Override
	public int compare(byte[] byte_array1, int start1, int length1, byte[] byte_array2, int start2, int length2) {
		int w1 = ByteUtils.ByteArrayToInt(byte_array1, start1);
		int w2 = ByteUtils.ByteArrayToInt(byte_array2, start2);

		int h1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 4);
		int h2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 4);

		int b1 = ByteUtils.ByteArrayToInt(byte_array1, start1 + 8);
		int b2 = ByteUtils.ByteArrayToInt(byte_array2, start2 + 8);

		int size1 = w1 * h1 * b1;
		int size2 = w2 * h2 * b2;

		return (size1 - size2);
	}

}
