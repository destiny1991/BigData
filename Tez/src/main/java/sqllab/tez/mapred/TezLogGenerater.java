package sqllab.tez.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;

import com.google.common.base.Preconditions;

import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TezLogGenerater extends Configured implements Tool {
	static String OUTPUT = "Output";
	static String Generation = "Generation";
	static String Reduction = "Reduction";
	private static final Logger LOG = LoggerFactory.getLogger(TezLogGenerater.class);

	public static class GenerateProcessor extends SimpleProcessor {
	  
		public GenerateProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getOutputs().size() == 1);
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(Reduction).getWriter();
      
			for(int i=0; i<100; i++) {
				kvWriter.write(new Text("key"), new Text("value"));
			}
		}
	}
  
	public static class ReduceProcessor extends SimpleMRProcessor {
		public ReduceProcessor(ProcessorContext context) {
			super(context);
	    }

	    @Override
	    public void run() throws Exception {
	    	Preconditions.checkArgument(getInputs().size() == 1);
	        Preconditions.checkArgument(getOutputs().size() == 1);
	    	KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(Generation).getReader();
	        KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
	        while (kvReader.next()) {
	        	Text key = (Text) kvReader.getCurrentKey();
	        	for (Object v : kvReader.getCurrentValues()) {
	        		kvWriter.write(key, (Text)v);
	        	}
	        }
	    }
	}
	
	public static class MyOutputFormat extends BaseOutputFormat<Text, Text> {
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value,
				Configuration conf) {
			return "read_write_hdfs.log";
		}
	}

	private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
		int parallelism) throws IOException {
	  
		DataSinkDescriptor redSink = MROutput.createConfigBuilder(new Configuration(tezConf), 
				MyOutputFormat.class, outputPath).build();

		Vertex genVertex = Vertex.create(Generation,
			  ProcessorDescriptor.create(GenerateProcessor.class.getName()), parallelism);
        
		Vertex redVertex = Vertex.create(Reduction, 
			  ProcessorDescriptor.create(ReduceProcessor.class.getName()), 1)
			  .addDataSink(OUTPUT, redSink);
    
		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
			  .newBuilder(Text.class.getName(), Text.class.getName(),
					  HashPartitioner.class.getName())
			  .setFromConfiguration(tezConf)
			  .build();
    
		// Create DAG and add the vertices and the edges.
		DAG dag = DAG.create("TezlogGenerater");
		dag.addVertex(genVertex)
      	 	.addVertex(redVertex)
      	 	.addEdge(
            Edge.create(genVertex, redVertex, edgeConf.createDefaultEdgeProperty()));

		return dag;  
	}

	public boolean run(String inputPath, String outputPath, Configuration conf,
			int parallelism) throws Exception {
		LOG.info("Running TezlogGenerater..");
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}
		tezConf.addResource(new Path("./src/main/resources/logConfig.xml"));
    
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.defaultFS", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    
//		tezConf.set("yarn.resourcemanager.hostname", "master");
//		tezConf.set("fs.defaultFS", "hdfs:///");
    
		UserGroupInformation.setConfiguration(tezConf);
    

		TezClient tezClient = TezClient.create("TezlogGenerater", tezConf);
		tezClient.start();
		try {
			DAG dag = createDAG(tezConf, inputPath, outputPath, parallelism);

			tezClient.waitTillReady();
			DAGClient dagClient = tezClient.submitDAG(dag);
			DAGStatus dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);

			if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
				LOG.error("DAG diagnostics: " + dagStatus.getDiagnostics());
				return false;
			}
			return true;
		} finally {
			// stop the client to perform cleanup
			tezClient.stop();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
   
		String inputPath = null;
		String outputPath = "F://TezOutput/";  	//local output path
		int parallelism = 4; 					//default parallelism of the program
		TezLogGenerater job = new TezLogGenerater();  
		LOG.info("Input path: " + inputPath + ", Output path: " + outputPath);
  
		if (job.run(inputPath, outputPath, conf, parallelism)) {
			return 0;
		}
    
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TezLogGenerater(), args);
		System.exit(res);
	}
}
