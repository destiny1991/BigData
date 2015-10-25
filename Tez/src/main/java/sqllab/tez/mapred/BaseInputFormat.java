package sqllab.tez.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BaseInputFormat extends InputFormat<Text, Text> {

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> result = new ArrayList<InputSplit>();
		Path outDir = FileOutputFormat.getOutputPath(job);

		int numSplits = job.getConfiguration().getInt("maps_num", 5);
		for (int i = 0; i < numSplits; ++i) {
			result.add(new FileSplit(new Path(outDir, i + ""), 0, 1,
					(String[]) null));
		}
		return result;
	}

	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new MyRecordReader(((FileSplit) split).getPath());
	}
}