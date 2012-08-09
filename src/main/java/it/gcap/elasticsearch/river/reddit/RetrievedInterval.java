package it.gcap.elasticsearch.river.reddit;

public class RetrievedInterval {

	private String firstId;
	private String lastId;
	private int count;

	public RetrievedInterval(String firstId, String lastId, int count) {
		this.firstId = firstId;
		this.lastId = lastId;
		this.count = count;
	}

	public String getFirstId() {
		return firstId;
	}

	public String getLastId() {
		return lastId;
	}
	
	public int getCount() {
		return count;
	}

}
