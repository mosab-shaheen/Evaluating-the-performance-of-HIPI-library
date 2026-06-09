package ProcessWithoutHIPI;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import ProcessWithHIPI.FloatImageContainer;
import hipi.image.FloatImage;
import hipi.image.ImageHeader;
import hipi.image.ImageHeader.ImageType;
import hipi.image.io.CodecManager;
import hipi.image.io.ImageDecoder;

/**
 * Provides the basic functionality of an ImageBundle record reader. Utilizes
 * {@link hipi.imagebundle.HipiImageBundle.FileReader} to read the portion of
 * the HipiImageBundle denoted by the InputSplit to get the ImageHeader and
 * FloatImage
 * 
 *
 */
public class ProcessRecordReaderWOH extends RecordReader<ImageHeader, FloatImageContainer> {

	private List<String> paths;
	private int processed;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {

		// Obtain path to input list of image URLs
		processed = 0;
		CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		Path[] path = combineFileSplit.getPaths();
		paths = new ArrayList<String>(path.length);
		String temp = new String();
		for (int i = 0; i < path.length; i++) {
			temp = path[i].toString();
			System.out.println(temp);
			int index = temp.indexOf(Path.SEPARATOR_CHAR);
			System.out.println(temp.substring(index));
			paths.add(temp.substring(index));
		}
	}

	@Override
	public void close() throws IOException {
		return;
	}

	@Override
	public ImageHeader getCurrentKey() throws IOException, InterruptedException {
		ImageType imageType = ImageType.UNSUPPORTED_IMAGE;
		System.out.println(paths.get(processed - 1));
		java.nio.file.Path path = Paths.get(paths.get(processed - 1));
		String type = Files.probeContentType(path);
		if (type.compareTo("image/jpeg") == 0) {
			imageType = ImageType.JPEG_IMAGE;
		} else if (type.compareTo("image/png") == 0) {
			imageType = ImageType.PNG_IMAGE;
		} else if (type.compareTo("image/ppm") == 0) {
			imageType = ImageType.PPM_IMAGE;
		} else if (type.compareTo("image/tiff") == 0) {
			imageType = ImageType.TIFF_IMAGE;
		}
		ImageDecoder decoder = CodecManager.getDecoder(imageType);
		if (decoder == null) {
			System.out.println("decoder is null");
			return null;
		}
		System.out.println(path.toString());
		InputStream input_stream = new FileInputStream(path.toString());
		ImageHeader _header = null;
		try {
			_header = decoder.decodeImageHeader(input_stream);
		} catch (Exception e) {
			e.printStackTrace();
			_header = null;
		}
		return _header;
	}

	@Override
	public FloatImageContainer getCurrentValue() throws IOException, InterruptedException {
		ImageType imageType = ImageType.UNSUPPORTED_IMAGE;
		java.nio.file.Path path = Paths.get(paths.get(processed - 1));
		String type = Files.probeContentType(path);
		if (type.compareTo("image/jpeg") == 0) {
			imageType = ImageType.JPEG_IMAGE;
		} else if (type.compareTo("image/png") == 0) {
			imageType = ImageType.PNG_IMAGE;
		} else if (type.compareTo("image/ppm") == 0) {
			imageType = ImageType.PPM_IMAGE;
		} else if (type.compareTo("image/tiff") == 0) {
			imageType = ImageType.TIFF_IMAGE;
		}
		ImageDecoder decoder = CodecManager.getDecoder(imageType);
		if (decoder == null) {
			System.out.println("decoder is null");
			return null;
		}
		InputStream input_stream = new FileInputStream(path.toString());
		FloatImage _image = null;
		try {
			_image = decoder.decodeImage(input_stream);
		} catch (Exception e) {
			e.printStackTrace();
			_image = null;
		}
		return new FloatImageContainer(_image);
	}

	@Override
	public float getProgress() throws IOException {
		return (((float) processed) / paths.size());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		boolean hasNext = (processed != paths.size());
		processed++;
		return hasNext;
	}
}
