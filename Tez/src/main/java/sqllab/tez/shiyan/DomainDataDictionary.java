package sqllab.tez.shiyan;

import java.io.File;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.teradata.sqllab.foundation.data.util.DataDictionary;
import com.teradata.sqllab.foundation.data.value.DataDictionaryValue;
import com.teradata.sqllab.foundation.data.value.IntegerValue;
import com.teradata.sqllab.foundation.data.value.StringValue;

public class DomainDataDictionary extends Domain<DataDictionaryValue, Integer> {

	private static final Map<String, Class<?>> TypeMap = new HashMap<String, Class<?>>();

	static {
		TypeMap.put("String", StringValue.class);
		TypeMap.put("Integer", IntegerValue.class);
	}

	private DataDictionary dataDictionary = new DataDictionary();

	private int startSequence = 0;

	public boolean setData(String lowerBound, String upperBound, Integer each) throws Exception {
		if (this.dataDictionary.isCaseSensitive() && upperBound.compareTo(lowerBound) < 0)
			throw new Exception("The lower bound is smaller than the upper bound!!");
		else if (upperBound.toLowerCase().compareTo(lowerBound.toLowerCase()) < 0)
			throw new Exception("The lower bound is smaller than the upper bound!!");
		int count = 0;
		String tmpValue = "";
		String tmpLowBound = this.dataDictionary.isCaseSensitive() ? lowerBound : lowerBound.toLowerCase();
		String tmpUpperBound = this.dataDictionary.isCaseSensitive() ? upperBound : upperBound.toLowerCase();
		boolean hasStartSequence = false;
		for (DataDictionaryValue ddv : this.dataDictionary.getValues()) {
			tmpValue = ddv.getValue();
			if (!this.dataDictionary.isCaseSensitive())
				tmpValue = tmpValue.toLowerCase();
			if (tmpValue.compareTo(tmpLowBound) < 0) {
				count++;
				continue;
			}
			if (tmpValue.compareTo(tmpUpperBound) > 0)
				break;
			if (!hasStartSequence) {
				this.startSequence = count;
				count = 0;
				hasStartSequence = true;
			}
			count++;
		}
		if (!hasStartSequence) {
			this.setValueCount(BigInteger.ZERO);
		} else {
			int valuecount = count / each;
			if (count % each != 0)
				valuecount++;
			this.setValueCount(BigInteger.valueOf(valuecount));
		}
		setEach(each);
		return true;
	}

	public DomainDataDictionary(String dictName, int length, boolean caseSensitive) throws Exception {
		this.dataDictionary.setName(dictName);
		Connection conn = DriverManager.getConnection("jdbc:derby:dataDictionary/javadb/data_dictionary;");
		PreparedStatement ps = conn.prepareStatement("select * from dictionary where name=?");
		ps.setString(1, dictName);
		ResultSet rs = ps.executeQuery();

		if (!rs.next()) {
			initDictionary();
			rs = ps.executeQuery();
			if (!rs.next())
				throw new Exception("There is no Data Dictionary with the name \"" + dictName + "\"");
		}

		this.dataDictionary.setId(rs.getInt("id"));
		this.dataDictionary.setCode(rs.getString("code"));
		this.dataDictionary.setTag(rs.getString("tag"));
		this.dataDictionary.setVtype(rs.getString("vtype"));
		this.dataDictionary.setCaseSensitive(caseSensitive);
		this.dataDictionary.setValueType(TypeMap.get(rs.getString("vtype")));

		if (caseSensitive)
			ps = conn.prepareStatement("select word from words where did=? order by word asc");
		else
			ps = conn.prepareStatement("select word from words where did=? order by lower(word) asc");
		ps.setInt(1, this.dataDictionary.getId());
		rs = ps.executeQuery();
		this.dataDictionary.getValues().clear();
		DataDictionaryValue ddv = null;
		while (rs.next()) {
			String value = rs.getString("word");
			// if (!caseSensitive)
			// value = value.toUpperCase();
			if (value.length() <= length) {
				DataDictionaryValue tmp = new DataDictionaryValue(value);
				if(tmp != null && tmp.getValue().contains("'"))
					tmp.setValue(tmp.toString().replace("'", "''"));
				if (ddv != null) {
					tmp.setPreValue(ddv);
					ddv.setNextValue(tmp);
				}
				this.dataDictionary.getValues().add(tmp);
				ddv = tmp;
			}
		}
		this.setValueCount(BigInteger.valueOf(this.dataDictionary.getValues().size()));
		rs.close();
		rs = null;
		ps.close();
		ps = null;
		conn.close();
		conn = null;
		setEach(1);
	}

	@SuppressWarnings("unchecked")
	public static boolean initDictionary() throws Exception {
		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		Document document = null;
		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			conn = DriverManager.getConnection("jdbc:derby:dataDictionary/javadb/data_dictionary;");
			document = new SAXReader().read(new File("dataDictionary/dataDictionary.xml"));
		} catch (DocumentException e) {
			throw new Exception("Error while loading Data Dictionary.", e);
		} catch (SQLException e) {
			throw new Exception("Error while getting connection with Dery database.", e);
		} catch (ClassNotFoundException e) {
			throw new Exception("Can't find the jdbc driver.", e);
		}

		int dictID = 0;
		String dictName = "";
		for (Element ele : (List<Element>) document.getRootElement().elements()) {
			dictName = ele.attribute("name").getValue();
			ps = conn.prepareStatement("select id from dictionary where name=?");
			ps.setString(1, dictName);
			rs = ps.executeQuery();
			if (!rs.next()) {
				ps = conn.prepareStatement("insert into dictionary (name,code,tag,vtype,casesensi) values (?, ?, ?, ?, ?)");
				ps.setString(1, dictName);
				ps.setString(2, ele.element("code").getText());
				ps.setString(3, ele.element("tag").getText());
				ps.setString(4, ele.elementText("vtype"));
				ps.setString(5, ele.elementText("case"));
				ps.executeUpdate();
				conn.commit();
			} else {
				ps = conn.prepareStatement("update dictionary set code=?,tag=?,vtype=?,casesensi=? where id=?");
				ps.setString(1, ele.element("code").getText());
				ps.setString(2, ele.element("tag").getText());
				ps.setString(3, ele.elementText("vtype"));
				ps.setString(4, ele.elementText("case"));
				ps.setInt(5, rs.getInt("id"));
				ps.executeUpdate();
				conn.commit();
			}
			ps = conn.prepareStatement("select id from dictionary where name=?");
			ps.setString(1, dictName);
			rs = ps.executeQuery();
			rs.next();
			dictID = rs.getInt("id");

			String content = FileUtils.readFileToString(new File("dataDictionary/" + dictName + ".txt"), Charset.forName("UTF8"));
			List<String> strValues = new ArrayList<String>(Arrays.asList(StringUtils.split(content, "\r\n")));
			ps = conn.prepareStatement("select count(*) as num from words where did=?");
			ps.setInt(1, dictID);
			rs = ps.executeQuery();
			rs.next();
			if (rs.getInt("num") != strValues.size()) {
				ps = conn.prepareStatement("delete from words where did=?");
				ps.setInt(1, dictID);
				ps.executeUpdate();
				for (int i = 0; i < strValues.size(); i++) {
					ps = conn.prepareStatement("insert into words (did,word) values (?, ?)");
					ps.setInt(1, dictID);
					ps.setString(2, strValues.get(i).trim());
					ps.executeUpdate();
				}
				conn.commit();
			}
		}

		rs.close();
		rs = null;
		ps.close();
		ps = null;
		conn.close();
		conn = null;
		return true;
	}

	@Override
	public DataDictionaryValue getValue(BigInteger index) {
		if (index.compareTo(this.getValueCount().subtract(BigInteger.ONE)) > 0 || (index.compareTo(BigInteger.ZERO) < 0))
			throw new IndexOutOfBoundsException("Index: " + index + ", From: 0, To: " + this.getValueCount().subtract(BigInteger.ONE));
		return this.dataDictionary.getValues().get(this.startSequence + index.intValue() * getEach());
	}

	public List<DataDictionaryValue> getValues(BigInteger count) {
		int dataDicValCount = dataDictionary.getValues().size();
		return this.dataDictionary.getValues().subList(this.startSequence, this.startSequence + count.intValue() > dataDicValCount ? dataDicValCount : count.intValue());
	}

	final static Class<?>[] compareAbleDomain = {};

	@Override
	public boolean isCompareableTo(Domain<?, ?> anotherDomain) {
		if (anotherDomain.getClass() == this.getClass())
			return true;
		if (Arrays.asList(compareAbleDomain).contains(anotherDomain.getClass()))
			return true;
		return false;
	}

	public static void main(String[] args) throws Exception {
		DomainDataDictionary domain = new DomainDataDictionary("Firstnames", 20, false);
		 domain.setData("b", "ZYLINA", 1);
		 System.out.println(domain.getValue(BigInteger.valueOf(369)));
		 System.out.println(domain.getValue(BigInteger.valueOf(370)));
		//for (int i = 0; i < domain.getValueCountInteger(); i++)
			//System.out.println(domain.getValue(BigInteger.valueOf(i)));
	}
}
