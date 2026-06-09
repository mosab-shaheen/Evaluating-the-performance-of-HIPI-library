package mosab.process.bundler;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.JPEGImageUtil;
import hipi.image.io.PNGImageUtil;
import hipi.image.io.PPMImageUtil;
import hipi.image.io.TIFFImageUtil;
import hipi.imagebundle.HipiImageBundle;

/**
 * A utility MapReduce program that takes a list of image URL's, downloads them, and creates a
 * {@link hipi.imagebundle.HipiImageBundle} from them.
 * 
 * When running this program, the user must specify 3 parameters. The first is the location of the
 * list of URL's (one URL per line), the second is the output path for the HIB that will be
 * generated, and the third is the number of nodes that should be used during the program's
 * execution. This final parameter should be chosen with respect to the total bandwidth your
 * particular cluster is able to handle. An example usage would be: <br />
 * <br />
 * bundler.jar /path/to/urls.txt /path/to/output.hib 10 <br />
 * /home/mosab/Desktop/input/1 /home/mosab/Desktop/output/HIB/output.hib 10
 * 
 * <br />
 * This program will automatically force 10 nodes to download the set of URL's contained in the
 * input list, thus if your list contains 100,000 images, each node in this example will be
 * responsible for downloading 10,000 images.
 *
 */
public class Bundler extends Configured implements Tool {

  public static class BundlerMapper extends Mapper<IntWritable, TextArrayWritable, BooleanWritable, Text> {

    // Download images at the list of input URLs and store them in a temporary HIB.
    @Override
    public void map(IntWritable key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
    	
    	Configuration conf = context.getConfiguration();
      // Create path for temporary HIB file
      String tempPath = conf.get("bundler.outpath") + key.get() + ".hib.tmp";
      HipiImageBundle hib = new HipiImageBundle(new Path(tempPath), conf);
      hib.open(HipiImageBundle.FILE_MODE_WRITE, true);

      // The value argument contains a list of image URLs delimited by \n. Setup buffered reader to allow processing this string line by line.
      //int k = key.get();
      int iprev = 0;
      FileSystem fileSystem= FileSystem.get(conf);
      // Iterate through URLs
      Text[] paths = (Text[])value.get();
      for (int i = 0; i < paths.length; i++) {
		

	// Put at most 100 images in a temporary HIB
        if (i >= iprev + 100) {
          hib.close();
          context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));
          tempPath = conf.get("bundler.outpath") + i + ".hib.tmp";
          hib = new HipiImageBundle(new Path(tempPath), conf);
          hib.open(HipiImageBundle.FILE_MODE_WRITE, true);
          iprev = i;
        }

     // Setup to time download
        long startT = 0;
        long stopT = 0;
        startT = System.currentTimeMillis();

        // Perform download and update HIB
        try {

          String type = "";
          DataInputStream fiStream;

	  // Attempt to download image at URL using java.net
          try {
        	Path file = new Path(paths[i].toString());
            
            System.err.println("Accessing " + file.toString());
            fiStream = new DataInputStream(fileSystem.open(file));
            java.nio.file.Path path = Paths.get(paths[i].toString());  
            System.out.println( path + " : " + Files.probeContentType(path) );
            type = Files.probeContentType(path);
          } catch (Exception e) {
            System.err.println("Error while trying to access: " + paths[i].toString());
            continue;
          }

	  // Check that image format is supported, header is parsable, and add to HIB if so
          if (type != null && (type.compareTo("image/jpeg") == 0 || type.compareTo("image/png") == 0 || type.compareTo("image/ppm") == 0 || type.compareTo("image/tiff") == 0)) {

	    // Get input stream for URL connection
	    InputStream bis = new BufferedInputStream(fiStream);

	    // Mark current location in stream for later reset
	    bis.mark(Integer.MAX_VALUE);

	    // Attempt to decode the image header
	    
	    ImageHeader header=null; 
	    ImageType imageType= ImageType.UNSUPPORTED_IMAGE;
	    
///////////////////////////////////////////////////////////////	    
    	// Get current size of heap in bytes
		long heapSize = Runtime.getRuntime().totalMemory();
		System.out.println("heapSize = " + heapSize);
		// Get maximum size of heap in bytes. The heap cannot grow beyond this
		// size.// Any attempt will result in an OutOfMemoryException.
		long heapMaxSize = Runtime.getRuntime().maxMemory();
		System.out.println("heapMaxSize = " + heapMaxSize);
		// Get amount of free memory within the heap in bytes. This size will
		// increase // after garbage collection and decrease as new objects are
		// created.
		long heapFreeSize = Runtime.getRuntime().freeMemory();
		System.out.println("heapFreeSize = " + heapFreeSize);
/////////////////////////////////////////////////////////////
		
		if (type.compareTo("image/jpeg") == 0){
	    	header = JPEGImageUtil.getInstance().decodeImageHeader(bis);
	    	imageType= ImageType.JPEG_IMAGE;
	    }else if	(type.compareTo("image/png") == 0){
	    	header = PNGImageUtil.getInstance().decodeImageHeader(bis);
	    	imageType= ImageType.PNG_IMAGE;
	    }else if	(type.compareTo("image/ppm") == 0){
	    	header = PPMImageUtil.getInstance().decodeImageHeader(bis);
	    	imageType= ImageType.PPM_IMAGE;
	    }else if	(type.compareTo("image/tiff") == 0){
	    	header = TIFFImageUtil.getInstance().decodeImageHeader(bis);
	    	imageType= ImageType.TIFF_IMAGE;
	    }
	    if (header == null)  {
	      System.err.println("Failed to parse header, not added to HIB: " + paths[i].toString());
	    } else {

	      // Passed header decode test, so reset to beginning of stream
	      bis.reset();

	      
	      // Add image to HIB
	      hib.addImage(bis, imageType);

	      System.err.println("Added to HIB: " + paths[i].toString());

	    }
          } else {
        	  System.err.println("Unsupported image format [" + type + "], not added to HIB: " + paths[i].toString());
	  }

        } catch (Exception e) {
          e.printStackTrace();
          System.err.println("Encountered error while trying to access: " + paths[i].toString());
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            ie.printStackTrace();
          }
        }

        //i++;

        // Report success and elapsed time
        stopT = System.currentTimeMillis();
        float el = (float) (stopT - startT) / 1000.0f;
        System.err.println("> Time elapsed " + el + " seconds");
      }

      try {

	// Output key/value pair to reduce layer consisting of boolean and path to HIB
        context.write(new BooleanWritable(true), new Text(hib.getPath().toString()));

        hib.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
  }

  public static class BundlerReducer extends
      Reducer<BooleanWritable, Text, BooleanWritable, Text> {

    private static Configuration conf;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      this.conf = context.getConfiguration();
    }

    // Combine HIBs produced by the map tasks into a single HIB
    @Override
    public void reduce(BooleanWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      
      if (key.get()) {

	// Get path to output HIB
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputHibPath = new Path(conf.get("bundler.outfile"));

	// Create HIB for writing
        HipiImageBundle hib = new HipiImageBundle(outputHibPath, conf);
        hib.open(HipiImageBundle.FILE_MODE_WRITE, true);

	// Iterate over the temporary HIB files created by map tasks
        for (Text tempString : values) {

	  // Open the temporary HIB file
          Path tempPath = new Path(tempString.toString());
          HipiImageBundle inputBundle = new HipiImageBundle(tempPath, conf);

	  // Append temporary HIB file to output HIB (this is fast)
          hib.append(inputBundle);

	  // Remove temporary HIB (both .hib and .hib.dat files)
          Path indexPath = inputBundle.getPath();
          Path dataPath = new Path(indexPath.toString() + ".dat");
          fileSystem.delete(indexPath, false);
          fileSystem.delete(dataPath, false);

	  // Emit output key/value pair indicating temporary HIB has been processed
          Text outputPath = new Text(inputBundle.getPath().toString());
          context.write(new BooleanWritable(true), outputPath);
          context.progress();
        }

	// Finalize output HIB
        hib.close();
      }
    }
  }


  public int run(String[] args) throws Exception {

    if (args.length < 2) {
    	System.out.println("Usage: bundler <input directory> <output HIB> <number of download nodes>");
        System.exit(0);
    }

    String inputFile = args[0];
    String outputFile = args[1];
    String outputPath = outputFile.substring(0, outputFile.lastIndexOf('/') + 1);
    int nodes = Integer.parseInt(args[2]);
    
    Configuration conf = super.getConf();
    
    //Attaching constant values to Configuration
    conf.setInt("bundler.nodes", nodes);
    conf.setStrings("bundler.outfile", outputFile);
    conf.setStrings("bundler.outpath", outputPath);

    Job job = Job.getInstance(conf, "Bundler");
    job.setJarByClass(Bundler.class);
    job.setMapperClass(BundlerMapper.class);
    job.setReducerClass(BundlerReducer.class);
    job.setInputFormatClass(BundlerInputFormat.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);

    FileSystem fs = FileSystem.get(job.getConfiguration());
	Path output_path = new Path(outputPath);
	if (fs.exists(output_path)) {
		fs.delete(output_path, true);
	}
	
    FileOutputFormat.setOutputPath(job, new Path(outputFile + "_output"));
    BundlerInputFormat.setInputPaths(job, new Path(inputFile));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new Bundler(), args);
    System.exit(res);
  }
}
