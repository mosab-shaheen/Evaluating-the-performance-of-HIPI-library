package mosab.process.ProcessWithoutHIPI;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import hipi.image.ImageHeader;
import mosab.common.FloatImageContainer;

public class ProcessInputFormatWOH extends FileInputFormat<ImageHeader, FloatImageContainer> {

	@Override
	public RecordReader<ImageHeader, FloatImageContainer> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new ProcessRecordReaderWOH();
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		Configuration conf = job.getConfiguration();
		int numMapTasks = conf.getInt("map.tasks", mosab.constants.ConstantManager.numMapTasks);
		Path output_path = FileOutputFormat.getOutputPath(job);
		// Create stub for temporary files
		FileSystem fileSystem = FileSystem.get(conf);

		fileSystem.mkdirs(output_path);
		// Initialize list to store output InputSplits
		List<InputSplit> splits = new ArrayList<InputSplit>();

		// Determine number of images to download (assume a single input text
		// file with one image URL per line)
		List<FileStatus> fileStatusList = listStatus(job);
		int numImages = fileStatusList.size();

		// Determine download schedule (number of images per node)
		int span = numImages / numMapTasks;
		int remaining = numImages % numMapTasks;
		// imagine I have 5 images and 4 maps then 6/4=1 and 6%4=2 so we give
		// every node 1 image and the last 2 nodes we give inc i.e 1 image extra
		if (numMapTasks > 1) {
			System.out.println("First " + (numMapTasks - remaining) + " nodes will each take " + span + " images");
			System.out.println("Last " + remaining + " nodes will each take " + (span+1) + " images");
			
		} else {
			System.out.println("remaining " + remaining + " images");
		}

		// Produce file splits according to download schedule

		int inc = 0;
		for (int j = 0; j < numMapTasks; j++) {

			if (j < numMapTasks - remaining) {
				Path[] path = new Path[span];
				long[] start = new long[span];
				long[] lengths = new long[span];
				List<String> locationsList = new ArrayList<String>();
				for (int k = 0; k < span; k++) {
					path[k] = fileStatusList.get((j * span) + k).getPath();
					start[k] = (j * span) + k;
					lengths[k] = fileStatusList.get((j * span) + k).getLen();

					FileStatus fs = fileStatusList.get((j * span) + k);
					long length = fileStatusList.get((j * span) + k).getLen();
					BlockLocation[] blocks = fileSystem.getFileBlockLocations(fs, 0, length);
					for (BlockLocation blockLocation : blocks) {
						for (String host : blockLocation.getHosts()) {
							if (!locationsList.contains(host)) {
								locationsList.add(host);
							}
						}
					}

				}
				String[] locations = locationsList.toArray(new String[locationsList.size()]);
				// String[] locations = new String[span];
				// Arrays.fill(locations, "");
				splits.add(new CombineFileSplit(path, start, lengths, locations));
			} else {
				Path[] path = new Path[span + 1];
				long[] start = new long[span + 1];
				long[] lengths = new long[span + 1];
				List<String> locationsList = new ArrayList<String>();
				for (int k = 0; k < span + 1; k++) {
					path[k] = fileStatusList.get((j * span) + k + inc).getPath();
					start[k] = 0;// (j * span) + k;
					lengths[k] = fileStatusList.get((j * span) + k + inc).getLen();

					FileStatus fs = fileStatusList.get((j * span) + k + inc);
					long length = fileStatusList.get((j * span) + k + inc).getLen();
					BlockLocation[] blocks = fileSystem.getFileBlockLocations(fs, 0, length);
					for (BlockLocation blockLocation : blocks) {
						for (String host : blockLocation.getHosts()) {
							if (!locationsList.contains(host)) {
								locationsList.add(host);
							}
						}
					}

				}
				String[] locations = locationsList.toArray(new String[locationsList.size()]);
				// String[] locations = new String[last];
				// Arrays.fill(locations, "");
				splits.add(new CombineFileSplit(path, start, lengths, locations));
				inc++;
			}
		}

		return splits;
	}

}
