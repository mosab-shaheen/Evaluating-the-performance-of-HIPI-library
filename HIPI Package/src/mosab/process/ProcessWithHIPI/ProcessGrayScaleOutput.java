package mosab.process.ProcessWithHIPI;

import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

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
import mosab.common.FloatImageContainer;
import mosab.kmeans.Cluster;
import mosab.kmeans.KMeans;
import mosab.kmeans.Point;
import mosab.kmeans.Segments;

// /home/mosab/Desktop/output/HIB/output.hib /home/mosab/Desktop/output/Process

//See the Covariance program in HIPI to know how to deal with the float image (generation, writing,etc.)
public class ProcessGrayScaleOutput extends Configured implements Tool {
	public static int k = 5;

	public static class MeanMapper extends Mapper<ImageHeader, FloatImageContainer, IntWritable, Segments> {
		@Override
		public void map(ImageHeader key, FloatImageContainer value, Context context)
				throws IOException, InterruptedException {

			KMeans kmeans = new KMeans();
			kmeans.init(value, k);
			Segments segments = kmeans.calculate();
//			byte[] pexils = ByteUtils.FloatArraytoByteArray(value.getFloatImage().getData());
//			String md5Str;
//			try {
//				md5Str = calculateMd5(pexils);
//			} catch (NoSuchAlgorithmException e) {
//				e.printStackTrace();
//				context.setStatus("Internal error - can't find the algorithm for calculating the md5");
//				return;
//			}
//			Text md5Text = new Text(md5Str);

			// put the file in the map where the md5 is the key, so duplicates
			// will
			// be grouped together for the reduce function

			IntWritable iw = new IntWritable(value.getFloatImage().getData().hashCode());
			context.write(iw, segments);

		}

//		static String calculateMd5(byte[] imageData) throws NoSuchAlgorithmException {
//			// get the md5 for this specific data
//			MessageDigest md = MessageDigest.getInstance("MD5");
//			md.update(imageData);
//			byte[] hash = md.digest();
//
//			// Below code of converting Byte Array to hex
//			String hexString = new String();
//			for (int i = 0; i < hash.length; i++) {
//				hexString += Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1);
//			}
//			return hexString;
//		}

	}

	public static class MeanReducer extends Reducer<IntWritable, Segments, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Segments> values, Context context)
				throws IOException, InterruptedException {

			for (Segments segments : values) {
				byte inc = 0;
				byte[] pixels = new byte[segments.getHeight() * segments.getWidth()];
				for (Cluster cluster : segments.getClusters()) {
					for (Point point : cluster.getPoints()) {
						int x =point.getPexilLocation().getJ();
						int y  = point.getPexilLocation().getI();
						
						pixels[point.getPexilLocation().getI() * segments.getWidth()
								+ point.getPexilLocation().getJ()] = inc;
					}
					inc += (byte) (255 / (k-1));
				}
				
				
				Path outputPath = new Path("/home/mosab/Desktop/output/segmented/" + key + " Gray.jpeg");

				Configuration conf = context.getConfiguration();
				FileSystem fs = FileSystem.get(conf);
				if (fs.exists(outputPath)) {
					System.err.println("output path exists");
					System.exit(1);
				}

				OutputStream os = fs.create(outputPath);
				
				ImageOutputStream ios = ImageIO.createImageOutputStream(os);
				Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("jpeg");
				ImageWriter writer = writers.next();
				writer.setOutput(ios);

				DataBuffer dataBuffer = new DataBufferByte(pixels, pixels.length);
				// set up offsets for the R,G,B elements
				final int[] offsets = new int[1];

				offsets[0] = 0;

				final WritableRaster writableRaster = Raster.createInterleavedRaster(dataBuffer, segments.getWidth(),
						segments.getHeight(), segments.getWidth(), 1, offsets, null);

				// Create the image
				BufferedImage bufferedImage = new BufferedImage(segments.getWidth(), segments.getHeight(),
						BufferedImage.TYPE_BYTE_GRAY);
				bufferedImage.setData(writableRaster);
				IIOImage iioImage = new IIOImage(bufferedImage, null, null);
				ImageWriteParam param = writer.getDefaultWriteParam();
				writer.write(null, iioImage, param);

			}

			// Text text = new Text("");
			// context.write(key, text);
		}
	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: FirstProgram <input HIB> <output directory>");
			System.exit(0);
		}

		Configuration conf = super.getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ProcessGrayScaleOutput.class);
		job.setMapperClass(MeanMapper.class);
		job.setReducerClass(MeanReducer.class);

		job.setInputFormatClass(ProcessInputFormatWH.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Segments.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().setBoolean("mapreduce.map.output.compress", true);
		job.getConfiguration().setInt("hipi.map.tasks", 2);
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
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new ProcessGrayScaleOutput(), args);
		System.exit(res);
	}

}
