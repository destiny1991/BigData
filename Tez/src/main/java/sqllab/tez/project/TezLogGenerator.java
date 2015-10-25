package sqllab.tez.project;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;
import com.teradata.sqllab.foundation.data.domain.DomainDataDictionary;
import com.teradata.sqllab.foundation.data.domain.DomainInteger;
import com.teradata.sqllab.foundation.data.domain.DomainIp4;
import com.teradata.sqllab.foundation.data.domain.DomainRegexString;
import com.teradata.sqllab.foundation.data.value.IntegerValue;
import com.teradata.sqllab.foundation.data.value.Ip4Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezLogGenerator extends Configured implements Tool {
	static String OUTPUT = "Output";
	static String Generation = "Generation";
	static String Reduction = "Reduction";
	private static final Logger LOG = LoggerFactory.getLogger(TezLogGenerator.class);

	public static class GenerateProcessor extends SimpleProcessor {
		long linesToWrite;
		long totalLines;
		int parallelism;
		String[] logFormat;
		String nickname;
		DomainDataDictionary dd1;
		DomainDataDictionary dd2;
		DomainDataDictionary dd3;
		DomainDataDictionary dd4;
		DomainDataDictionary dd5;
		
		public GenerateProcessor(ProcessorContext context) {
			super(context);
		}
		
	    private void generateLogFormat(String str) {
			if (str == null || str.length() == 0)
				return;

			str = str.trim();
			int k = str.length() - 1;
			while (k>=0 && str.charAt(k) != '"')
				k--;
			if(k <= 1) return;
			nickname = str.substring(k + 1).trim();
			str = str.substring(1, k);
			logFormat = str.split(" ");
		}
	    
	    private String getVar(String str) {
			StringBuffer sb = new StringBuffer();
			boolean flag = false;
			for (int i = str.length() - 1; i >= 0; i--) {
				char ch = str.charAt(i);
				if (flag && !Character.isLetter(ch))
					break;
				if (Character.isLetter(ch)) {
					sb.append(ch);
					flag = true;
				}
			}
			return sb.reverse().toString();
		}
	    
	    private String getModifier(String str) {
			for(int i=0; i<str.length(); i++)
				if(str.charAt(i) == '%') {
					if(i+1 < str.length()) {
						if(str.charAt(i+1) == '-') return "-";
						else if(str.charAt(i+1) == '>') return ">";
						else if(str.charAt(i+1) == '<') return "<";
					}
				}
			return null;
		}
		
		private String getPreSuf(String str) {
			int k = 0;
			char ch = str.charAt(k);
			String res = "";
			while(ch == '\\') {
				res += str.charAt(k+1);
				k += 2;
				ch = str.charAt(k);
			}
			return res;
		}    
	    
	    private String getFoobar(String str) {
			StringBuffer sb = new StringBuffer();
			int k = 0;
			while(k < str.length() && str.charAt(k) != '{') k++;
			if(k >= str.length()) return "";
					
			k++;
			while(str.charAt(k) != '}') {
				sb.append(str.charAt(k));
				k++;
			}
			return sb.toString();
		}
	    
	    private String getStrRandom(String[] strs) {
			int n = strs.length;
			double num = 1.0/n;
			double tmp = Math.random();
			for(int i=0; i<n; i++) {
				if(num*(i+1)>tmp) return strs[i];
			}
			return strs[n-1];
		}
	    
		private Text generateKey(Timestamp time) {
			/*
			StringBuffer sentence = new StringBuffer();
			long order = time.getTime();
			sentence.append(order);
			return new Text("[" + sentence.toString() + "]");
			*/
			
			DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return new Text("[" + df1.format(time) + "]");
		}
	    
		private Text generateValue(String[] format) {
			StringBuffer sentence = new StringBuffer();
			String var = null;
			String modifier = null;
			String preSuf = null;
			String foobar = "";
			IntegerValue num;
			DomainIp4 dip = new DomainIp4();
			DomainInteger di = new DomainInteger();
			
			if(format == null) return new Text(sentence.toString().trim());
			
			for (String str : format) {
				modifier = getModifier(str);
				
				//modifier "-" decide if we will display this variable
				if(modifier == "-") {
					sentence.append("- ");
					continue;
				}
				
				preSuf = getPreSuf(str);
				var = getVar(str);
				switch (var) {
					case "a":
						dip.setData(new Ip4Value("11.0.0.0"), new Ip4Value("191.167.255.555"), 1);
						sentence.append(preSuf + dip.getValueRandom() + preSuf + " ");
						//sentence.append(preSuf + "RemoteIP-address" + preSuf + " ");
						break;
					case "A":
						dip.setData(new Ip4Value("192.168.0.0"), new Ip4Value("192.168.255.255"), 1);
						sentence.append(preSuf + dip.getValueRandom() + preSuf + " ");
						//sentence.append(preSuf + "LocalIP-address" + preSuf + " ");
						break;
					case "B":
						di.setData(0, 65535, 1);
						num = di.getValueRandom();
						sentence.append(preSuf + num + "Bytes" + preSuf + " ");
						//sentence.append(preSuf + "SizeOfResponse(0)" + preSuf + " ");
						break;
					case "b":
						di.setData(0, 65535, 1);
						num = di.getValueRandom();
						if(0 == num.getValue())
							sentence.append(preSuf + "-" + preSuf + " ");
						else
							sentence.append(preSuf + num + "Bytes" + preSuf + " ");
						//sentence.append(preSuf + "SizeOfResponse(-)" + preSuf + " ");
						break;
					case "C":
						foobar = getFoobar(str);
						try {
							DomainRegexString drs = new DomainRegexString();
							drs.setData("A", "zzzzzzzzzzzzzzz", "[A-Z]{1,15}");
							sentence.append(preSuf + foobar + "=" + drs.getValueRandom().toOriginalString() + preSuf + " ");
						} catch (Exception e) {
							e.printStackTrace();
						}
						//sentence.append(preSuf + foobar + "TheContentsOfCookieFoobar" + preSuf + " ");
						break;
					case "D":
						di.setData(10, 30000, 1);						
						sentence.append(preSuf + di.getValueRandom() + "ms" + preSuf + " ");
						//sentence.append(preSuf + "TimeTakenToServeTheRequest" + preSuf + " ");
						break;
					case "e":						
						foobar = getFoobar(str);
						Map env = System.getenv();
						Properties pro = System.getProperties();
						pro.putAll(env);
						sentence.append(preSuf + pro.get(foobar) + preSuf + " ");
						//sentence.append(preSuf + foobar + "TheContentsOfTheEnvironmentVariableFOOBAR" + preSuf + " ");
						break;
					case "f":
						String[] fileType = {".txt", ".html", ".css", ".js", ".docx", ".java"};
						sentence.append(preSuf + 
									getStrRandom(new String[]{dd1.getValueRandom().toOriginalString(), 
										dd2.getValueRandom().toOriginalString()})
									+ getStrRandom(fileType) + preSuf + " ");
						//sentence.append(preSuf + "Filename" + preSuf + " ");
						break;
					case "h":
						dip.setData(new Ip4Value("11.0.0.0"), new Ip4Value("191.167.255.555"), 1);
						sentence.append(preSuf + dip.getValueRandom() + preSuf + " ");
						//sentence.append(preSuf + "RemoteHost" + preSuf + " ");
						break;
					case "H":
						String[] protocol = {"htpp", "htpps", "ftp"};
						String theRequestProtocol = getStrRandom(protocol);
						sentence.append(preSuf + theRequestProtocol + preSuf + " ");
						//sentence.append(preSuf + "TheRequestProtocol" + preSuf + " ");
						break;
					case "i":
						foobar = getFoobar(str);
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + foobar + "HeaderLine(s)InTheRequest" + preSuf + " ");
						break;
					case "k":
						di.setData(0, 10000, 1);
						sentence.append(preSuf + di.getValueRandom() + "(alives)" + preSuf + " ");
						//sentence.append(preSuf + "NumberOfKeepaliveRequestsHandled" + preSuf + " ");
						break;
					case "l":
						sentence.append(preSuf + dd4.getValueRandom().toOriginalString() + " "
								+ dd5.getValueRandom().toOriginalString()
								+ preSuf + " ");
						//sentence.append(preSuf + "RemoteLogname" + preSuf + " ");
						break;
					case "m":
						String[] httpAccessMethod = {
							"OPTIONS", "GET", "HEAD", "POST", "PUT", "DELETE", "TRACE", "CONNECT"
							};
						sentence.append(preSuf + getStrRandom(httpAccessMethod) + preSuf + " ");
						//sentence.append(preSuf + "TheRequestMethod" + preSuf + " ");
						break;
					case "n":
						foobar = getFoobar(str);
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + foobar + "TheContentsOfNoteFoobar" + preSuf + " ");
						break;
					case "o":
						foobar = getFoobar(str);
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + foobar + "HeaderLine(s)InTheReply" + preSuf + " ");
						break;
					case "p":
						foobar = getFoobar(str);
						di.setData(20, 5000, 1);
						sentence.append(preSuf + di.getValueRandom() + preSuf + " ");
						//if(foobar != "") foobar += ":";
						//sentence.append(preSuf + foobar + "TheCanonicalPortOfTheServer" + preSuf + " ");
						break;
					case "P":
						foobar = getFoobar(str);
						di.setData(1000, 65535, 1);
						sentence.append(preSuf + di.getValueRandom() + preSuf + " ");
						//if(foobar != "") foobar += ":";
						//sentence.append(preSuf + foobar + "TheProcessIDOfTheChild" + preSuf + " ");
						break;
					case "q":
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + "?The query string." + preSuf + " ");
						break;
					case "r":
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + "First line of request." + preSuf + " ");
						break;
					case "R":
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + "The handler generating the response." + preSuf + " ");
						break;
					case "s":
						if(modifier == ">") {
							di.setData(200, 206, 1);
							sentence.append(preSuf + di.getValueRandom() + preSuf + " ");
							//sentence.append(preSuf + "TheStatusOfTheLastRequest" + preSuf + " ");
						} else {
							di.setData(400, 417, 1);
							sentence.append(preSuf + di.getValueRandom() + preSuf + " ");
							//sentence.append(preSuf + "TheStatusOfTheOriginalRequest" + preSuf + " ");
						}
						break;
					case "t":
						Date dd = new Date();
						foobar = getFoobar(str);
						if(foobar != "") {
							long ms = dd.getTime();
							if(foobar.compareTo("sec") == 0) ms /= 1000;
							if(foobar.compareTo("usec") == 0) ms *= 1000;
							sentence.append(preSuf + ms + preSuf + " ");
						} else {
							sentence.append(preSuf + dd + preSuf + " ");
						}
						//sentence.append(preSuf + foobar + "TimeTheRequestWasReceived" + preSuf + " ");
						break;
					case "T":
						foobar = getFoobar(str);
						di.setData(0, 600, 1); //second: 0~600s
						int tmp = di.getValueRandom().getValue();
						if(0 == foobar.compareTo("ms")) {
							tmp *= 1000;
						}
						else if(0 == foobar.compareTo("us")) {
							tmp *= 1000000;
						}
						sentence.append(preSuf + tmp + foobar + preSuf + " ");
						//sentence.append(preSuf + foobar + "TheTimeTakenToServeTheRequest" + preSuf + " ");
						break;
					case "u":
						sentence.append(preSuf + dd4.getValueRandom().toOriginalString() + " "
								+ dd5.getValueRandom().toOriginalString()
								+ preSuf + " ");
						//sentence.append(preSuf + "RemoteUser" + preSuf + " ");
						break;
					case "U":
						dip.setData(new Ip4Value("11.0.0.0"), new Ip4Value("191.167.255.555"), 1);
						sentence.append(preSuf + dip.getValueRandom() + preSuf + " ");
						//sentence.append(preSuf + "TheURLPathRequested" + preSuf + " ");
						break;
					case "v":
						String[] urlType = {"com", "edu.cn", "org", "xyz", "net"};
						try {
							DomainRegexString drs = new DomainRegexString();
							drs.setData("A", "zzzzzzzzzzzzzzz", "[w]{3}\\.[a-z]{3,8}\\.");
							sentence.append(preSuf + drs.getValueRandom().toOriginalString()+
									getStrRandom(urlType) +preSuf + " ");
						} catch (Exception e) {
							e.printStackTrace();
						}
						//sentence.append(preSuf + "TheCanonicalServerName" + preSuf + " ");
						break;
					case "V":
						String[] urlType1 = {"com", "edu.cn", "org", "xyz", "net"};
						try {
							DomainRegexString drs = new DomainRegexString();
							drs.setData("A", "zzzzzzzzzzzzzzz", "[w]{3}\\.[a-z]{3,8}\\.");
							sentence.append(preSuf + drs.getValueRandom().toOriginalString()+
									getStrRandom(urlType1) + ":80" +preSuf + " ");
						} catch (Exception e) {
							e.printStackTrace();
						}
						//sentence.append(preSuf + "TheServerName" + preSuf + " ");
						break;
					case "X":
						String[] statusType = {"X", "+", "-"};
						sentence.append(preSuf + getStrRandom(statusType) + preSuf + " ");
						//sentence.append(preSuf + "ConnectionStatus" + preSuf + " ");
						break;
					case "I":
						di.setData(0, 65535, 1);
						sentence.append(preSuf + di.getValueRandom() + "Bytes" + preSuf + " ");
						//sentence.append(preSuf + "BytesReceived" + preSuf + " ");
						break;
					case "O":
						di.setData(0, 65535, 1);
						sentence.append(preSuf + di.getValueRandom() + "Bytes" + preSuf + " ");
						//sentence.append(preSuf + "BytesSent" + preSuf + " ");
						break;
					case "ti":
						foobar = getFoobar(str);
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + foobar + "TrailerLine(s)InTheRequest" + preSuf + " ");
						break;
					case "to":
						foobar = getFoobar(str);
						sentence.append(preSuf + dd3.getValueRandom().toOriginalString() 
								+ preSuf + " ");
						//sentence.append(preSuf + foobar + "TrailerLine(s)InTheResponse" + preSuf + " ");
						break;
					default:
						break;
				}
			}
			return new Text(sentence.toString().trim());
		}
	    
		@Override
	    public void initialize() throws Exception {
	        Configuration conf = new Configuration();
	        conf.addResource(new Path("./src/main/resources/logConfig.xml"));								
	        totalLines = conf.getLong("totalLines", 3000);
	        parallelism = getContext().getVertexParallelism();
	        linesToWrite = totalLines / parallelism;
	        try {
				DomainDataDictionary.initDictionary();
				dd1 = new DomainDataDictionary("Englishwords", 200, true);
				dd2 = new DomainDataDictionary("Englishwords_lower", 200, true);
				dd3 = new DomainDataDictionary("Sentence", 200, true);
				dd4 = new DomainDataDictionary("Lastnames", 200, true);
				dd5 = new DomainDataDictionary("Firstnames", 200, true);
			} catch (Exception e) {
				e.printStackTrace();
			}
	        generateLogFormat(conf.get("logFormat"));
	    }
		
		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getOutputs().size() == 1);
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(Reduction).getWriter();
			
			Date date = new Date();
		    Calendar cal = Calendar.getInstance();
		    cal.setTime(date);
		    while (linesToWrite > 0) {
		    	Timestamp time = new Timestamp(cal.getTime().getTime());
		    	Text keyWords = generateKey(time);
		    	Text valueWords = null;
		    	valueWords = generateValue(logFormat);
		    	kvWriter.write(keyWords, valueWords);

		    	cal.add(Calendar.SECOND, 1);
		    	linesToWrite--;
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

	private DAG createDAG(TezConfiguration tezConf, String inputPath, String outputPath,
		int parallelism) throws IOException {

		DataSinkDescriptor redSink = MROutput.createConfigBuilder(new Configuration(tezConf), 
				TextOutputFormat.class, outputPath).build();

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
    
		DAG dag = DAG.create("TezlogGeneration");
		dag.addVertex(genVertex)
      	 	.addVertex(redVertex)
      	 	.addEdge(
            Edge.create(genVertex, redVertex, edgeConf.createDefaultEdgeProperty()));

		return dag;  
	}

	public boolean run(String inputPath, String outputPath, Configuration conf) throws Exception {
		LOG.info("Running TezlogGenerator..");
		TezConfiguration tezConf;
		if (conf != null) {
			tezConf = new TezConfiguration(conf);
		} else {
			tezConf = new TezConfiguration();
		}
		//tezConf.addResource(new Path("./src/main/resources/logConfig.xml"));
		tezConf.addResource("logConfig.xml");
		
		/**
		 * decide outputfile whether to be compress;
		 */
		tezConf.setBoolean(FileOutputFormat.COMPRESS, 
				tezConf.getBoolean("isCompress", false));
		
		/**
		 * decide outputfile name;
		 */
		tezConf.set("mapreduce.output.basename", 
				tezConf.get("fileName", "Tez.log"));
		
		
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.defaultFS", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
		
//		tezConf.set("yarn.resourcemanager.hostname", "master");
//		tezConf.set("fs.defaultFS", "hdfs:///");		
		
		UserGroupInformation.setConfiguration(tezConf);
    
		TezClient tezClient = TezClient.create("TezlogGenerator", tezConf);
		tezClient.start();
		try {
			int parallelism = tezConf.getInt("parallelism", 2);
			outputPath = tezConf.get("outputPath", outputPath);
			
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
			tezClient.stop();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String inputPath = null;
		
		String outputPath = "F://TezOutput/";  	//local output path
		
		TezLogGenerator job = new TezLogGenerator();  
		LOG.info("Input path: " + inputPath + ", Output path: " + outputPath);
  
		if (job.run(inputPath, outputPath, conf)) {
			return 0;
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TezLogGenerator(), args);
		System.exit(res);
	}
}
