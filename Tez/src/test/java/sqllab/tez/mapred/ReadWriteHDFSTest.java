package sqllab.tez.mapred;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;

import sqllab.tez.mapred.ReadWriteHDFS;

/**
 * @author zc255005
 *
 */
public class ReadWriteHDFSTest {
	ReadWriteHDFS.MyMapper myMapper = null;
	
	@Before
	public void setUp() throws Exception {
		myMapper = new ReadWriteHDFS.MyMapper();
	}

	/**
	 *  @test: all formats
	 */
	@Test
	public void testGenerateValue1() throws Exception {
		Method method = myMapper.getClass().getDeclaredMethod("generateValue", String[].class);
		method.setAccessible(true);
		
		String str = "%a %A %B %b %C %D %e %f %h %H %i %k %l %m %n "
				+ "%o %p %P %q %r %R %s %t %T %u %U %v %V %X %I %O %ti %to";
		String[] formats = str.split(" ");
		Object res = method.invoke(myMapper,  (Object)formats);
		
		StringBuffer sb = new StringBuffer();
		sb.append("RemoteIP-address");
		sb.append(" LocalIP-address");
		sb.append(" SizeOfResponse(0)");
		sb.append(" SizeOfResponse(-)");
		sb.append(" TheContentsOfCookieFoobar");
		sb.append(" TimeTakenToServeTheRequest");
		sb.append(" TheContentsOfTheEnvironmentVariableFOOBAR");
		sb.append(" Filename");
		sb.append(" RemoteHost");
		sb.append(" TheRequestProtocol");
		sb.append(" HeaderLine(s)InTheRequest");
		sb.append(" NumberOfKeepaliveRequestsHandled");
		sb.append(" RemoteLogname");
		sb.append(" TheRequestMethod");
		sb.append(" TheContentsOfNoteFoobar");
		sb.append(" HeaderLine(s)InTheReply");
		sb.append(" TheCanonicalPortOfTheServer");
		sb.append(" TheProcessIDOfTheChild");
		sb.append(" TheQueryString");
		sb.append(" FirstLineOfRequest");
		sb.append(" HandlerGeneratingTheResponse");
		sb.append(" TheStatusOfTheOriginalRequest");
		sb.append(" TimeTheRequestWasReceived");
		sb.append(" TheTimeTakenToServeTheRequest");
		sb.append(" RemoteUser");
		sb.append(" TheURLPathRequested");
		sb.append(" TheCanonicalServerName");
		sb.append(" TheServerName");
		sb.append(" ConnectionStatus");
		sb.append(" BytesReceived");
		sb.append(" BytesSent");
		sb.append(" TrailerLine(s)InTheRequest");
		sb.append(" TrailerLine(s)InTheResponse");
		
		assertNotNull(res.toString());
		//assertEquals(res.toString(), sb.toString());
	}
	
	
	/**
	 * @test: %{..}..
	 */
	@Test
	public void testGenerateValue2() throws Exception {
		Method method = myMapper.getClass().getDeclaredMethod("generateValue", String[].class);
		method.setAccessible(true);
		
		String str = "%{Foobar}C %{FOOBAR}e %{Foobar}i %{Foobar}n %{Foobar}o "
				+ "%{format}p %{format}P %{format}t %{UNIT}T %{VARNAME}^ti %{VARNAME}^to";
		String[] formats = str.split(" ");
		Object res = method.invoke(myMapper,  (Object)formats);
		
		StringBuffer sb = new StringBuffer();
		sb.append("Foobar:TheContentsOfCookieFoobar");
		sb.append(" FOOBAR:TheContentsOfTheEnvironmentVariableFOOBAR");
		sb.append(" Foobar:HeaderLine(s)InTheRequest");
		sb.append(" Foobar:TheContentsOfNoteFoobar");
		sb.append(" Foobar:HeaderLine(s)InTheReply");
		sb.append(" format:TheCanonicalPortOfTheServer");
		sb.append(" format:TheProcessIDOfTheChild");
		sb.append(" format:TimeTheRequestWasReceived");
		sb.append(" UNIT:TheTimeTakenToServeTheRequest");
		sb.append(" VARNAME:TrailerLine(s)InTheRequest");
		sb.append(" VARNAME:TrailerLine(s)InTheResponse");
		
		assertNotNull(res.toString());
		//assertEquals(res.toString(), sb.toString());
	}
	
	
	/**
	 * @test: ", -, >, combined form
	 */
	@Test
	public void testGenerateValue3() throws Exception {
		Method method = myMapper.getClass().getDeclaredMethod("generateValue", String[].class);
		method.setAccessible(true);
		String str = "%-h %l %u %t \\\"%r\\\" %>s %b \\\"%i\\\" "
				+ "\\\"%{User-Agent}i\\\" %{VARNAME}^ti \\\"%h\\\" \\\"%-{format}h\\\"";
		String[] formats = str.split(" ");
		Object res = method.invoke(myMapper,  (Object)formats);
		
		StringBuffer sb = new StringBuffer();
		sb.append("-");
		sb.append(" RemoteLogname");
		sb.append(" RemoteUser");
		sb.append(" TimeTheRequestWasReceived");
		sb.append(" \"FirstLineOfRequest\"");
		sb.append(" TheStatusOfTheLastRequest");
		sb.append(" SizeOfResponse(-)");
		sb.append(" \"HeaderLine(s)InTheRequest\"");
		sb.append(" \"User-Agent:HeaderLine(s)InTheRequest\"");
		sb.append(" VARNAME:TrailerLine(s)InTheRequest");
		sb.append(" \"RemoteHost\"");
		sb.append(" -");
		
		assertNotNull(res.toString());
		//assertEquals(res.toString(), sb.toString());
	}
	
	
	/**
	 * @test: null
	 */
	@Test
	public void testGenerateValue4() throws Exception {
		Method method = myMapper.getClass().getDeclaredMethod("generateValue", String[].class);
		method.setAccessible(true);

		String[] formats = null;
		
		Object res = method.invoke(myMapper,  (Object)formats);		
		StringBuffer sb = new StringBuffer();
		assertEquals(res.toString(), sb.toString());
	}
	
	
	@Test
	public void testGenerateLogFormat1() throws Exception {
		String str = "\"%h %l %u %t \\\"%r\\\" %>s %b \\\"%{Referer}i\\\" \\\"%{User-agent}i\\\"\" combined";
		
		Method testNoParamMethod = myMapper.getClass().getDeclaredMethod("generateLogFormat",String.class);   
		testNoParamMethod.setAccessible(true);
		
		testNoParamMethod.invoke(myMapper, (Object)str);
		
		assertEquals(myMapper.nickname, "combined");
		
		String[] words = {"%h", "%l", "%u", "%t", "\\\"%r\\\"", "%>s", 
				"%b", "\\\"%{Referer}i\\\"", "\\\"%{User-agent}i\\\""};
		assertArrayEquals(myMapper.logFormat, words);
	}
	
	@Test
	public void testGenerateLogFormat2() throws Exception {
		String str = "\"%h %l %u %t \\\"%r\\\" %>s %b \\\"%{Referer}i\\\" \\\"%{User-agent}i\\\"\" ";
		
		Method testNoParamMethod = myMapper.getClass().getDeclaredMethod("generateLogFormat",String.class);   
		testNoParamMethod.setAccessible(true);
		
		testNoParamMethod.invoke(myMapper, (Object)str);
		assertEquals(myMapper.nickname, "");
		String[] words = {"%h", "%l", "%u", "%t", "\\\"%r\\\"", "%>s", 
				"%b", "\\\"%{Referer}i\\\"", "\\\"%{User-agent}i\\\""};
		assertArrayEquals(myMapper.logFormat, words);
	}
	
	@Test
	public void testGenerateLogFormat3() throws Exception {
		String str = "";
		
		Method testNoParamMethod = myMapper.getClass().getDeclaredMethod("generateLogFormat",String.class);   
		testNoParamMethod.setAccessible(true);
		
		testNoParamMethod.invoke(myMapper, (Object)str);
		
		assertEquals(myMapper.nickname, null);
		assertArrayEquals(myMapper.logFormat, null);
	}
}
