import java.awt.Frame;
import java.awt.RenderingHints;
import java.awt.image.DataBuffer;
import java.awt.image.renderable.ParameterBlock;
import java.io.IOException;
import javax.media.jai.JAI;
import javax.media.jai.LookupTableJAI;
import javax.media.jai.RenderedOp;
import com.sun.media.jai.codec.FileSeekableStream;
import com.sun.media.jai.codec.TIFFDecodeParam;
import javax.media.jai.widget.ScrollingImagePanel;

public class JAITest {
	// The main method.
	public static void main(String[] args) {
		
		// Create an input stream from the specified file name to be
		// used with the TIFF decoder.
		FileSeekableStream stream = null;
		try {
			stream = new FileSeekableStream("/home/mosab/Downloads/sample.tiff");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
		// Store the input stream in a ParameterBlock to be sent to
		// the operation registry, and eventually to the TIFF
		// decoder.
		ParameterBlock params = new ParameterBlock();
		params.add(stream);
		// Specify to TIFF decoder to decode images as they are and
		// not to convert unsigned short images to byte images.
		TIFFDecodeParam decodeParam = new TIFFDecodeParam();
		decodeParam.setDecodePaletteAsShorts(true);
		// Create an operator to decode the TIFF file.
		RenderedOp image1 = JAI.create("tiff", params);
		// Find out the first image’s data type.
		int dataType = image1.getSampleModel().getDataType();
		RenderedOp image2 = null;
		if (dataType == DataBuffer.TYPE_BYTE) {
			// Display the byte image as it is.
			System.out.println("TIFF image is type byte.");
			image2 = image1;
		} else if (dataType == DataBuffer.TYPE_USHORT) {
			// Convert the unsigned short image to byte image.
			System.out.println("TIFF image is type ushort.");
			// Setup a standard window-level lookup table. */
			byte[] tableData = new byte[0x10000];
			for (int i = 0; i < 0x10000; i++) {
				tableData[i] = (byte) (i >> 8);
			}
			// Create a LookupTableJAI object to be used with the
			// "lookup" operator.
			LookupTableJAI table = new LookupTableJAI(tableData);
			// Create an operator to lookup image1.
			image2 = JAI.create("lookup", image1, table);
		} else {
			System.out.println("TIFF image is type " + dataType
					+ ", and will not be displayed.");
			System.exit(0);
		}
		// Get the width and height of image2.
		int width = image2.getWidth();
		int height = image2.getHeight();
		// Attach image2 to a scrolling panel to be displayed.
		ScrollingImagePanel panel = new ScrollingImagePanel(image2, width,
				height);
		// Create a frame to contain the panel.
		Frame window = new Frame("Lookup Sample Program");
		window.add(panel);
		window.pack();
		window.show();
	}
}