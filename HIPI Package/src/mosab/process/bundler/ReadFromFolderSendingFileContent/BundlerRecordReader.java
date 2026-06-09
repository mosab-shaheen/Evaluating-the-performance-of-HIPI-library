package mosab.process.bundler.ReadFromFolderSendingFileContent;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Treats keys as index into training array and value as the training vector.
 */
public class BundlerRecordReader extends RecordReader<IntWritable, Text> {
	
	Path[] path;
	long[] start;
	long[] lengths;
	//String[] locations;
	int count;
	FileSystem fileSystem;
	private static int key=0;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

		// Obtain path to input list of image URLs
		CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		path = combineFileSplit.getPaths();
		start = combineFileSplit.getStartOffsets();
		lengths = combineFileSplit.getLengths();
		count = 0;
		fileSystem = path[0].getFileSystem(context.getConfiguration());
	}

	/**
	 * Get the progress within the split
	 */
	@Override
	public float getProgress() {
		if (count!=path.length) {
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
	public Text getCurrentValue() throws IOException, InterruptedException {
		byte[] contents = new byte[(int) lengths[count-1]];
		Text fileAsText = new Text();
        FSDataInputStream in = null;
        try {
          // Set the contents of this file.
          in = fileSystem.open(path[count-1]);
          IOUtils.readFully(in, contents, 0, contents.length);
          
          fileAsText.set(contents, 0, contents.length);

        } finally {
          IOUtils.closeStream(in);
        }
		return fileAsText;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (count!=path.length) {
			count++;
			return true;
		} else {
			return false;
		}
	}

}
