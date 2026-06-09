package mosab.process.imagesFromHib;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import hipi.image.ImageHeader;
import hipi.imagebundle.mapreduce.ImageBundleInputFormat;

public class ImageFromHibInputFormat extends FileInputFormat<ImageHeader, BytesWritable> {

  @Override
  public RecordReader<ImageHeader, BytesWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new ImageFromHibRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    // See ImageBundleInputFormat.java
    return ImageBundleInputFormat.computeSplits(jobContext, listStatus(jobContext));
  }

}
