package sequence.bundler;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// /home/mosab/Desktop/input/1 /home/mosab/Desktop/output/ProcessWS/sequence.seq

public class Bundler {

	public static void main(String[] args) throws IOException {
		String inString = args[0];
		String outString = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(inString), conf);
		Path inPath = new Path(inString);
		Path outPath = new Path(outString);
		if (fs.exists(outPath)) {
			fs.delete(outPath);
		}
		FileStatus[] status = fs.listStatus(inPath);
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		FSDataInputStream in = null;
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(fs, conf, outPath, key.getClass(), value.getClass());
		for (int i = 0; i < status.length; i++) {
			in = fs.open(status[i].getPath());
			byte buffer[] = new byte[in.available()];
			in.read(buffer);
			String type = Files.probeContentType(Paths.get(status[i].getPath().getName()));
			writer.append(new Text(type + ":" + inPath.getName()), new BytesWritable(buffer));
		}
		System.out.println("Finished Work");
	}
}
