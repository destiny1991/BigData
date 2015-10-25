package sqllab.tez.shiyan;

import java.math.BigInteger;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.teradata.sqllab.foundation.data.value.*;
import com.teradata.sqllab.foundation.data.domain.*;

public class Try {
	static Logger LOG = LoggerFactory.getLogger(Try.class);
	
	public static void main(String[] args) throws Exception {
		//TimestampValue from = new TimestampValue(new Date(), 0);	
		//TimestampValue to = new TimestampValue(new Date(), 0);
		//PeriodTimestampValue p = new PeriodTimestampValue(from, to, 0);
		
		/*
		TimeValue from = new TimeValue("10:10:10.200", 0);
		TimeValue to = new TimeValue("10:10:11.300", 0);
		System.out.println(to.minus(from));
		from.setMicroseconds(200);
		System.out.println(from.getMicroseconds());
		*/
		
	/*
		Map map = System.getenv();
		Iterator it = map.entrySet().iterator();
		while(it.hasNext()) {
			Entry entry = (Entry)it.next();
			System.out.print(entry.getKey()+"=");
			System.out.println(entry.getValue());
		}
		
		Properties properties = System.getProperties();
		it =  properties.entrySet().iterator();
		while(it.hasNext()) {
			Entry entry = (Entry)it.next();
			System.out.print(entry.getKey()+"=");
			System.out.println(entry.getValue());
		}
		*/
		
		/*
		Map env = System.getenv();
		Properties pro = System.getProperties();
		pro.putAll(env);
		System.out.println(pro.get("user.name"));
		*/
		
		/*
		 DomainInteger di = new DomainInteger();
		 di.setData(1, 20, 1);
		IntegerValue d = di.getValueRandom();
		System.out.println(d);
		 */
		
		/*
		 DomainTime dt = new DomainTime();
		//day, hour, minute, second, millisecond
		dt.setData(new TimeValue("10:10:10.20", 0), new TimeValue("20:10:20.50", 0), 1, "millisecond");
		for(int i=0; i<100; i++)
			System.out.println(dt.getValueRandom());
		 */
		
		/*
		DomainIp4 di = new DomainIp4();
		Ip4Value lb = new Ip4Value("10.0.0.0");
		Ip4Value ub = new Ip4Value("10.10.10.10");
		di.setData(new Ip4Value("10.0.0.0"), new Ip4Value("10.10.10.10"), 1);
		
		for(int i=0; i<1500; i++) {
			System.out.println(di.getValueRandom());
		}
		*/
		
		/*
		DomainInteger di = new DomainInteger();
		di.setData(10, 20, 1);
		di.setData(0, Integer.MAX_VALUE, 1);
		
		for(int i=0; i<1000; i++) {
			System.out.println(di.getValueRandom());
		}
		*/
		/*
		TimestampValue from = new TimestampValue(new Date(), 0);	
		System.out.println(from.getMicroseconds());
		*/
		
		/*
		 	TimeValue from = new TimeValue("10:10:10", 0);
			TimeValue to = new TimeValue("10:10:11", 0);
			sentence.append(preSuf + to.minus(from) + preSuf + " ");
		 */
		
		/*
		DomainRegexString drs = new DomainRegexString();
		// drs.setData("a", "e", "[a-e]");
		// for (int i = 0; i < drs.getValueCountInteger(); i++) {
		// System.out.println(drs.getValue(i));
		// }
		//drs.setData("0.0.0.0", "255.255.255.255", "[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");
		//drs.setData("0.0.0.0", "255.255.255.255", "[1-2]{0,1}[-5]{0,1}");
		//drs.setData("A", "zzzzzzzzzzzzzzz", "[A-Za-z]{1}[0-9A-Za-z]{0,14}");
		
		
		
		drs.setData("A", "zzzzzzzzzzzzzzz", "[A-Za-z]{1}[0-9A-Za-z]{0,9}");
		
		drs.setData("A", "z", "[A-Za-z]{1}");
		
		System.out.println(drs.getValueCountInteger() + " " + drs.getValueCount());
		//System.out.println(drs.getValue(BigInteger.valueOf(1L)));
		
		for(int i=0; i<1000; i++) { 
			//LOG.info(drs.getValue(i).toString());
			System.out.println(drs.getValueRandom());	 
		}
		*/
		
		/*
		TimestampValue tsv = new TimestampValue(new Date(), 0);
		Date dd = new Date();
		System.out.println(dd);
	*/	
		
		DomainDataDictionary.initDictionary();
		DomainDataDictionary dd = new DomainDataDictionary("Colors", 200, true);
		System.out.println(dd.getValueRandom());
		
		
		String a = String.valueOf(10);
		System.out.println(a.length());
	}

}
