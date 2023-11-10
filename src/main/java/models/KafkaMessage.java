package models;

public class KafkaMessage {
	public EntityFieldValue before;
	public EntityFieldValue after;
	public KafkaSourceData source;
	public String op;
	public Long ts_ms;
	public String transaction;
}
