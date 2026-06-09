package mosab.process.ProcessWithHIPI;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.curator.framework.state.ConnectionStateManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.ImageHeader;
import hipi.util.ByteUtils;
import mosab.common.FloatImageContainer;
import mosab.constants.ConstantManager;
import mosab.kmeans.Cluster;
import mosab.kmeans.KMeans;
import mosab.kmeans.Point;
import mosab.kmeans.Segments;

// HIB/5/output.hib Output/ProcessWH/5

//See the Covariance program in HIPI to know how to deal with the float image (generation, writing,etc.)
public class ProcessWH extends Configured implements Tool {
	public static int k = 5;
	public static Integer counter = 0;

	public static class MeanMapper extends Mapper<ImageHeader, FloatImageContainer, Text, Segments> {
		@Override
		public void map(ImageHeader key, FloatImageContainer value, Context context)
				throws IOException, InterruptedException {
			try {
				KMeans kmeans = new KMeans();
				kmeans.init(value, k);
				Segments segments = kmeans.calculate();
				byte[] pexils = ByteUtils.FloatArraytoByteArray(value.getFloatImage().getData());
				// String md5Str;
				// try {
				// md5Str = calculateMd5(pexils);
				// } catch (NoSuchAlgorithmException e) {
				// e.printStackTrace();
				// context.setStatus("Internal error - can't find the algorithm
				// for
				// calculating the md5");
				// return;
				// }
				// Text md5Text = new Text(md5Str);

				Text md5Text;
				synchronized (this) {
					counter++;
					md5Text = new Text(counter.toString());
				}

				// put the file in the map where the md5 is the key, so
				// duplicates
				// will
				// be grouped together for the reduce function

				// IntWritable iw = new
				// IntWritable(value.getFloatImage().getData().hashCode());
				context.write(md5Text, segments);
			} catch (Exception e) {
				System.out.println("MeanMapper map");
			}
		}

		// static String calculateMd5(byte[] imageData) throws
		// NoSuchAlgorithmException {
		// // get the md5 for this specific data
		// MessageDigest md = MessageDigest.getInstance("MD5");
		// md.update(imageData);
		// byte[] hash = md.digest();
		//
		// // Below code of converting Byte Array to hex
		// String hexString = new String();
		// for (int i = 0; i < hash.length; i++) {
		// hexString += Integer.toString((hash[i] & 0xff) + 0x100,
		// 16).substring(1);
		// }
		// return hexString;
		// }
	}

	public static class MeanReducer extends Reducer<Text, Segments, IntWritable, Text> {

		@Override
		public void reduce(Text key, Iterable<Segments> values, Context context)
				throws IOException, InterruptedException {
			try {
				List<Color> colors = new ArrayList<Color>(k);
				colors.add(new Color(255, 0, 0));
				colors.add(new Color(0, 255, 0));
				colors.add(new Color(0, 0, 255));
				colors.add(new Color(255, 255, 0));
				colors.add(new Color(0, 0, 0));
				for (Segments segments : values) {
					byte inc = 0;
					byte[] pixels = new byte[segments.getHeight() * segments.getWidth() * 3];
					for (Cluster cluster : segments.getClusters()) {
						for (Point point : cluster.getPoints()) {
							pixels[(point.getPexilLocation().getI() * segments.getWidth()
									+ point.getPexilLocation().getJ()) * 3 + 0] = (byte) colors.get(inc).getRed();
							pixels[(point.getPexilLocation().getI() * segments.getWidth()
									+ point.getPexilLocation().getJ()) * 3 + 1] = (byte) colors.get(inc).getGreen();
							pixels[(point.getPexilLocation().getI() * segments.getWidth()
									+ point.getPexilLocation().getJ()) * 3 + 2] = (byte) colors.get(inc).getBlue();
						}
						inc++;
					}

					Configuration conf = context.getConfiguration();
					FileSystem fs = FileSystem.get(conf);
					String fileName = conf.get("bundler.outpath") + "/" + key;
					Path outputPath = new Path(fileName + ".jpeg");

					while (fs.exists(outputPath)) {
						fs.delete(outputPath);
						// System.out.println("output path exists, changing name
						// ...");
						// fileName+=" ";
						// outputPath = new Path(fileName+ ".jpeg");
					}

					OutputStream os = fs.create(outputPath);

					ImageOutputStream ios = ImageIO.createImageOutputStream(os);
					Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
					ImageWriter writer = writers.next();
					writer.setOutput(ios);

					DataBuffer dataBuffer = new DataBufferByte(pixels, pixels.length);
					// set up offsets for the R,G,B elements
					final int[] offsets = new int[3];

					offsets[0] = 0;
					offsets[1] = 1;
					offsets[2] = 2;

					final WritableRaster writableRaster = Raster.createInterleavedRaster(dataBuffer,
							segments.getWidth(), segments.getHeight(), segments.getWidth() * 3, 3, offsets, null);

					// Create the image
					BufferedImage bufferedImage = new BufferedImage(segments.getWidth(), segments.getHeight(),
							BufferedImage.TYPE_INT_RGB);
					bufferedImage.setData(writableRaster);
					IIOImage iioImage = new IIOImage(bufferedImage, null, null);
					ImageWriteParam param = writer.getDefaultWriteParam();
					writer.write(null, iioImage, param);

					ios.flush();
					ios.close();
					writer.dispose();
				}
			} catch (Exception e) {
				System.out.println("MeanReducer reduce");
			}
			// Text text = new Text("");
			// context.write(key, text);
		}
	}

	public int run(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: FirstProgram <input HIB> <output directory> [<num map tasks> <num reduce tasks>]");
			System.exit(0);
		}

		Configuration conf = super.getConf();

		// Attaching constant values to Configuration
		conf.setStrings("bundler.outpath", args[1]);

		Job job = Job.getInstance(conf, "ProcessWH");
		job.setJarByClass(ProcessWH.class);
		job.setMapperClass(MeanMapper.class);
		job.setReducerClass(MeanReducer.class);

		job.setInputFormatClass(ProcessInputFormatWH.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Segments.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);

		int numMapTasks = ConstantManager.numMapTasks;
		int numReduceTasks = ConstantManager.numReduceTasks;
		try {
			if (args.length >= 3) {
				numMapTasks = Integer.parseInt(args[2]);
			}
			if (args.length >= 4) {
				numReduceTasks = Integer.parseInt(args[3]);
			}
		} catch (Exception e) {
		}

		job.getConfiguration().setInt("map.tasks", numMapTasks);
		job.setNumReduceTasks(numReduceTasks);

		job.setSpeculativeExecution(true);

		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path output_path = new Path(args[1]);
		if (fs.exists(output_path)) {
			fs.delete(output_path, true);
		}

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		int res = 0;
		try {
			Configuration conf = new Configuration();
			res = ToolRunner.run(conf, new ProcessWH(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("start " + start + "End " + end + "Duration " + (end - start) + "ms");
		System.exit(res);
	}

}
