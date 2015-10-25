package sqllab.tez.mapred;

import java.io.IOException;
import java.net.URI;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.teradata.sqllab.foundation.data.domain.DomainDataDictionary;
import com.teradata.sqllab.foundation.data.domain.DomainInteger;
import com.teradata.sqllab.foundation.data.domain.DomainIp4;
import com.teradata.sqllab.foundation.data.domain.DomainRegexString;
import com.teradata.sqllab.foundation.data.value.IntegerValue;
import com.teradata.sqllab.foundation.data.value.Ip4Value;

public class ReadWriteHDFS extends Configured implements Tool {
	static final String OUTPUT_PATH = "hdfs://master:8020/tmp/MapRedOutput/";

	static class MyMapper extends Mapper<Text, Text, Text, Text> {
		long linesToWrite;
		long totalLines;
		int maps_num;
		String[] logFormat = null;
		String nickname = null;
		DomainDataDictionary dd1;
		DomainDataDictionary dd2;
		DomainDataDictionary dd3;
		DomainDataDictionary dd4;
		DomainDataDictionary dd5;
		
		/**
		 * Initialize some important parameters. 
		 * Called once at the beginning of the task.
		 */
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			linesToWrite = conf.getLong("linesPerMap", 5); // default 5 words per map
														
			totalLines = conf.getLong("totalLines", 3000);
			maps_num = conf.getInt("maps_num", 3);
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
		
		/**
		 * Each map generate linesToWrite words, but the last map will
		 * generate (linesToWrite + remainder) words, 
		 * (remainder = totalLines % maps_num)
		 */
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Date date = new Date();
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);

			if (totalLines % maps_num != 0
					&& maps_num == Long.parseLong(key.toString()) + 1)
				linesToWrite += totalLines % maps_num;

			while (linesToWrite > 0) {
				Timestamp time = new Timestamp(cal.getTime().getTime());
				Text keyWords = generateKey(time);
				Text valueWords = null;
				valueWords = generateValue(logFormat);
				context.write(keyWords, valueWords);

				cal.add(Calendar.SECOND, 1);
				linesToWrite--;
			}
		}
		
		/**
		 * Transform the timestamp to the given form,
		 * and use it as the key.
		 * @param time
		 * @return key
		 */
		private Text generateKey(Timestamp time) {
			//uisng Timestamp format
			//StringBuffer sentence = new StringBuffer();
			//long order = time.getTime();
			//sentence.append(order);
			DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return new Text("[" + df1.format(time) + "]");
		}
		
		/**
		 * Get the format variable from each format item, like "%{Foobar}C"
		 * then "C" will return.
		 * @param str
		 * @return format variable string
		 */
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
		
		/**
		 * Get modifier like '-', '>', '<' form each format item,
		 * these moidifer restricted the returning result of the format variable.
		 * @param str
		 * @return modifier, if the modifier don't exist, return null.
		 */
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
		
		/**
		 * Get prefix form each format item, 
		 * we suppose the suffix match a pair with the prefix.
		 * What's more, each prefix or suffix must be added Escape character in the head.
		 * @param str
		 * @return prefix(suffix)
		 */
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
		
		/**
		 * Some format item may contain foobar, like "%{Foobar}C", "%{format}t",
		 * this function will return the foobar from the variable.
		 * if the foobar don't exit, an empty string will be returned.
		 * @param str
		 * @return foobar
		 */
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
		
		/**
		 * Split the string which is from the configuration file to 
		 * String array.
		 * @param str
		 */
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

		/**
		 * Randomly choose a word from the given string list.
		 * @param strs
		 * @return
		 */
		private String getStrRandom(String[] strs) {
			int n = strs.length;
			double num = 1.0/n;
			double tmp = Math.random();
			for(int i=0; i<n; i++) {
				if(num*(i+1)>tmp) return strs[i];
			}
			return strs[n-1];
		}
		
		/**
		 * generate value according to logFormat
		 * @param format
		 * @return
		 */
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
	}
	
	/**
	 * define output file format and name
	 * @author zc255005
	 */
	public static class MyOutputFormat extends
			BaseOutputFormat<Text, Text> {
		@Override
		protected String generateFileNameForKeyValue(Text key, Text value,
				Configuration conf) {
			return "read_write_hdfs.log";
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		// if the output dir exits, then delete it.
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		Path outpath = new Path(OUTPUT_PATH);
		if (fileSystem.exists(outpath))
			fileSystem.delete(outpath, true);

		// load config file
		conf.addResource(new Path("./src/main/resources/logConfig.xml"));
		long totalLines = conf.getLong("totalLines", 3000);
		int mapsNum = conf.getInt("maps_num", 3);
		long linesPerMap = totalLines / mapsNum;

		if (totalLines < mapsNum) {
			conf.setLong("maps_num", 1);
			conf.setLong("linesPerMap", totalLines);
		} else {
			conf.setLong("maps_num", mapsNum);
			conf.setLong("linesPerMap", linesPerMap);
		}

		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadWriteHDFS.class);
		job.setJobName("ReadWriteHDFS");
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(BaseInputFormat.class);
		job.setOutputFormatClass(MyOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(1); // merge into one file

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(new Configuration(), new ReadWriteHDFS(), args);
		System.exit(res);
	}
}
