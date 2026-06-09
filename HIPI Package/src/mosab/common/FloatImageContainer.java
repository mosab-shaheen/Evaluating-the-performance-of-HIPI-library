package mosab.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

import hipi.image.FloatImage;

public class FloatImageContainer implements Writable, RawComparator<BinaryComparable> {

	private FloatImage floatImage;

	public FloatImage getFloatImage() {
		return floatImage;
	}

	public void setFloatImage(FloatImage floatImage) {
		this.floatImage = floatImage;
	}

	public FloatImageContainer() {
		this.floatImage = new FloatImage();
	}

	public FloatImageContainer(FloatImage floatImage) {
		this.floatImage = floatImage;
	}

	@Override
	public int compare(BinaryComparable o1, BinaryComparable o2) {
		// TODO Auto-generated method stub
		return floatImage.compare(o1, o2);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		// TODO Auto-generated method stub
		return floatImage.compare(b1, s1, l1, b2, s2, l2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		floatImage.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		floatImage.readFields(in);
	}

}
