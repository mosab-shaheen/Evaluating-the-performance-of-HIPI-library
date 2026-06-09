package mosab.process.bundler.ReadFromFolderSendingFileContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class BundlerInputFormat extends FileInputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new BundlerRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		Configuration conf = job.getConfiguration();

		// Use a default value of 10 if 'bundler.nodes' is not explicitly set
		int numDownloadNodes = conf.getInt("bundler.nodes", 10);

		// Initialize list to store unique nodes in cluster
		ArrayList<String> uniqueNodes = new ArrayList<String>(0);

		// Initialize list to store output InputSplits
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Create stub for temporary files
		FileSystem fileSystem = FileSystem.get(conf);
		String tempOutputPath = conf.get("bundler.outpath") + "_tmp";
		Path tempOutputDir = new Path(tempOutputPath);

		// Ensure clean temporary directory
		if (fileSystem.exists(tempOutputDir)) {
			fileSystem.delete(tempOutputDir, true);
		}
		fileSystem.mkdirs(tempOutputDir);

		// Search for numDownloadNodes unique nodes on the cluster by creating
		// up to (2*numDownloadNodes) temporary files on the HDFS and seeing
		// where they land.
		// Please visit http://hipi.cs.virginia.edu/examples/bundler.html for
		// a detailed description.
		int i = 0;
		while (uniqueNodes.size() < numDownloadNodes && i < 2 * numDownloadNodes) {

			// Create temporary file
			String tempFileString = tempOutputPath + "/" + i;
			Path tempFile = new Path(tempFileString);
			FSDataOutputStream os = fileSystem.create(tempFile);
			os.write(i);
			os.close();

			// Retrieve block locations of temporary file
			FileStatus match = fileSystem.getFileStatus(tempFile);
			long length = match.getLen();
			BlockLocation[] blocks = fileSystem.getFileBlockLocations(match, 0, length);

			// Check if the first node used to store this temporary file is not
			// yet on our list
			boolean save = true;
			for (int j = 0; j < uniqueNodes.size(); j++) {
				if (blocks[0].getHosts()[0].compareTo(uniqueNodes.get(j)) == 0) {
					save = false;
					System.out.println("Repeated host: " + i);
					break;
				}
			}

			// If unique, add it to list of unique nodes
			if (save) {
				uniqueNodes.add(blocks[0].getHosts()[0]);
				System.out.println("Found unique host: " + i);
			}
			i++;
		}

		System.out.println(
				"Tried to get " + numDownloadNodes + " unique nodes, found " + uniqueNodes.size() + " unique nodes.");

		// Determine number of images to download (assume a single input text
		// file with one image URL per line)
		List<FileStatus> fileStatusList = listStatus(job);
		FileStatus file = fileStatusList.get(0);
		int numImages = fileStatusList.size();
		;

		// Determine download schedule (number of images per node)
		int span = (int) Math.ceil(((float) numImages) / ((float) uniqueNodes.size()));
		int last = numImages - span * (uniqueNodes.size() - 1);

		if (uniqueNodes.size() > 1) {
			System.out.println("First " + (uniqueNodes.size() - 1) + " nodes will each download " + span + " images");
			System.out.println("Last node will download " + last + " images");
		} else {
			System.out.println("Single node will download " + last + " images");
		}

		// Produce file splits according to download schedule
		int numUniqueNodes = uniqueNodes.size();
		for (int j = 0; j < numUniqueNodes; j++) {

			String node = uniqueNodes.get(j);
			if (j < numUniqueNodes - 1) {
				Path[] path = new Path[span];
				long[] start = new long[span];
				long[] lengths = new long[span];
				String[] locations = new String[span];
				for (int k = 0; k < span; k++) {
					path[k] = fileStatusList.get((j * span) + k).getPath();
					start[k] = 0;//(j * span) + k;
					lengths[k] = fileStatusList.get((j * span) + k).getLen();
					locations[k] = node;
				}
				splits.add(new CombineFileSplit(path, start, lengths, locations));
			} else {
				Path[] path = new Path[last];
				long[] start = new long[last];
				long[] lengths = new long[last];
				String[] locations = new String[last];
				for (int k = 0; k < last; k++) {
					path[k] = fileStatusList.get((j * span) + k).getPath();
					start[k] = 0;//(j * span) + k;
					lengths[k] = fileStatusList.get((j * span) + k).getLen();
					locations[k] = node;
				}
				splits.add(new CombineFileSplit(path, start, lengths, locations));
			}
		}

		// Remove temporary files used to identify unique nodes in cluster
		if (fileSystem.exists(tempOutputDir)) {
			fileSystem.delete(tempOutputDir, true);
		}

		return splits;
	}

}
