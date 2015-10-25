package sqllab.tez.shiyan;

import java.math.BigInteger;
import java.util.Random;

public abstract class Domain<T extends Comparable<? super T>, U> {
	private String name;
	private String type;
	private String sampleDataType;
	private BigInteger valueCount;

	private T lowerBound;
	private T upperBound;
	private U each;

	private String interval;
	protected int durationEach;
	protected String durationInterval;

	Domain() {
	}

	Domain(String name, String type, String sampleDataType) {
		this.name = name;
		this.type = type;
		this.sampleDataType = sampleDataType;
	}

	public BigInteger getValueCount() { // number of values
		return this.valueCount;
	}

	public int getValueCountInteger() {
		if (isOverIntegerRange())
			return Integer.MAX_VALUE;
		return getValueCount().intValue();
	}

	public boolean isOverIntegerRange() {
		return getValueCount().compareTo(BigInteger.valueOf(getValueCount().intValue())) != 0;
	}

	public abstract T getValue(BigInteger Ith);

	public T getValue(int Ith) {
		return getValue(BigInteger.valueOf(Ith));
	}

	/**
	 * Returns a value randomly within the value interval
	 * 
	 * @return Returns a value randomly
	 */
	public T getValueRandom() {
		if (this.valueCount.compareTo(BigInteger.ZERO) <= 0)
			return getValue(BigInteger.ZERO);
		Random rand = new Random();
		BigInteger result = new BigInteger(this.valueCount.bitLength(), rand);
		while (result.compareTo(this.valueCount) >= 0) {
			result = new BigInteger(this.valueCount.bitLength(), rand);
		}
		return getValue(result);
	}

	public abstract boolean isCompareableTo(Domain<?, ?> anotherDomain);

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getSampleDataType() {
		return sampleDataType;
	}

	public void setSampleDataType(String sampleDataType) {
		this.sampleDataType = sampleDataType;
	}

	public T getLowerBound() {
		return lowerBound;
	}

	public void setLowerBound(T lowerBound) {
		this.lowerBound = lowerBound;
	}

	public T getUpperBound() {
		return upperBound;
	}

	public void setUpperBound(T upperBound) {
		this.upperBound = upperBound;
	}

	public U getEach() {
		return each;
	}

	public void setEach(U each) {
		this.each = each;
	}

	public void setValueCount(BigInteger valueCount) {
		this.valueCount = valueCount;
	}

	public String getInterval() {
		return interval;
	}

	public void setInterval(String interval) {
		this.interval = interval;
	}

	public int getDurationEach() {
		return durationEach;
	}

	public void setDurationEach(int durationEach) {
		this.durationEach = durationEach;
	}

	public String getDurationInterval() {
		return durationInterval;
	}

	public void setDurationInterval(String durationInterval) {
		this.durationInterval = durationInterval;
	}

}
