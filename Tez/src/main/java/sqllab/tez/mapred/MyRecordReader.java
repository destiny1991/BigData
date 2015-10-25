package sqllab.tez.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyRecordReader extends RecordReader<Text, Text> {
	Path path;
	Text key = null;
	Text value = new Text();

	public MyRecordReader(Path p) {
		path = p;
	}

	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
	}

	public boolean nextKeyValue() {
		if (path != null) {
			key = new Text();
			key.set(path.getName());
			path = null;
			return true;
		}
		return false;
	}

	public Text getCurrentKey() {
		return key;
	}

	public Text getCurrentValue() {
		return value;
	}

	public void close() {
	}

	public float getProgress() {
		return 0.0f;
	}
}
