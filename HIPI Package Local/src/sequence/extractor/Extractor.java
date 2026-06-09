package sequence.extractor;

import java.io.FileOutputStream;
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

// /home/mosab/Desktop/output/ProcessWS/sequence.seq /home/mosab/Desktop/output/ProcessWSExt/

public class Extractor {

	public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException {
		String inString = args[0];
		String outString = args[1];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(inString), conf);
		Path inPath = new Path(inString);
		Path outPath = new Path(outString);
		if (fs.exists(outPath)) {
			fs.delete(outPath);
		}
		
		SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), inPath, conf);
		Text key = (Text) reader.getKeyClass().newInstance();
		BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();
		while (reader.next(key, value)){
			
			
		}
		  // perform some operating
		reader.close();
		
		System.out.println("Finished Work");
	}
}
