package models;

import java.math.BigInteger;

public class KafkaSourceData {
    public String version; 
    public String connector;
    public String name;
    public BigInteger ts_ms;
    public String snapshot;
    public String db;
    public String[] sequence;
    public String schema;
    public String table;
    public String txId;
    public String lsn;
    public String xmin;
}
