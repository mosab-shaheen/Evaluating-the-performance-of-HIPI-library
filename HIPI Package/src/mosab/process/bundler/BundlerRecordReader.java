package mosab.process.bundler;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Treats keys as index into training array and value as the training vector.
 */
public class BundlerRecordReader extends RecordReader<IntWritable, TextArrayWritable> {

	private boolean singletonEmit;
	private TextArrayWritable textArrayWritable;
	private static int key = 0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

		// Obtain path to input list of image URLs
		CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		Path[] path = combineFileSplit.getPaths();
		String[] paths = new String [path.length];
		String temp = new String();
		for (int i = 0; i < path.length; i++) {
			temp = path[i].toString();
			int index = temp.indexOf(Path.SEPARATOR_CHAR);
			paths[i]=temp.substring(index);
		}
		textArrayWritable = new TextArrayWritable(paths);
		
		// Flag to enable emitting only one key/value pair
		singletonEmit = false;
	}

	/**
	 * Get the progress within the split
	 */
	@Override
	public float getProgress() {
		if (singletonEmit) {
			return 1.0f;
		} else {
			return 0.0f;
		}
	}

	@Override
	public void close() throws IOException {
		return;
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		synchronized (this) {
			key++;
			return new IntWritable(key);
		}

	}

	@Override
	public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
		return textArrayWritable;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (singletonEmit == false) {
			singletonEmit = true;
			return true;
		} else {
			return false;
		}
	}

}
