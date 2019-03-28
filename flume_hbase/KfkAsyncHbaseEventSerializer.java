package org.apache.flume.sink.hbase;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import java.util.ArrayList;
import java.util.List;

public class KfkAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {
    private byte[] table;
    private byte[] cf;
    private byte[] payload;
    private byte[] payloadColumn;
    private byte[] incrementColumn;
    private String rowPrefix;
    private byte[] incrementRow;
    private KeyType keyType;

    @Override
    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.cf = cf;
    }

    @Override
    public List<PutRequest> getActions() {
        List<PutRequest> actions = new ArrayList<PutRequest>();
        if (payloadColumn != null) {
            byte[] rowKey;
            try {

                String[] columns = new String(this.payloadColumn).split(",");
                String[] values = new String(this.payload).split(",");

                for (int i=0; i < columns.length; i++) {
                    byte[] colColumn = columns[i].getBytes();
                    byte[] colValue = values[i].getBytes(Charsets.UTF_8);

                    String datetime = values[0].toString();
                    String userid =  values[1].toString();
                    rowKey = SimpleRowKeyGenerator.getKfkRowKey(userid, datetime);
                    PutRequest putRequest =  new PutRequest(table, rowKey, cf,
                            colColumn, colValue);
                    actions.add(putRequest);
                }
            } catch (Exception e) {
                throw new FlumeException("Could not get row key!", e);
            }
        }
        return actions;
    }

    public List<AtomicIncrementRequest> getIncrements() {
        List<AtomicIncrementRequest> actions = new ArrayList<AtomicIncrementRequest>();
        if (incrementColumn != null) {
            AtomicIncrementRequest inc = new AtomicIncrementRequest(table,
                    incrementRow, cf, incrementColumn);
            actions.add(inc);
        }
        return actions;
    }

    @Override
    public void cleanUp() {
        // TODO Auto-generated method stub

    }

    @Override
    public void configure(Context context) {
        String pCol = context.getString("payloadColumn", "pCol");
        String iCol = context.getString("incrementColumn", "iCol");
        rowPrefix = context.getString("rowPrefix", "default");
        String suffix = context.getString("suffix", "uuid");
        if (pCol != null && !pCol.isEmpty()) {
            if (suffix.equals("timestamp")) {
                keyType = KeyType.TS;
            } else if (suffix.equals("random")) {
                keyType = KeyType.RANDOM;
            } else if (suffix.equals("nano")) {
                keyType = KeyType.TSNANO;
            } else {
                keyType = KeyType.UUID;
            }
            payloadColumn = pCol.getBytes(Charsets.UTF_8);
        }
        if (iCol != null && !iCol.isEmpty()) {
            incrementColumn = iCol.getBytes(Charsets.UTF_8);
        }
        incrementRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
    }

    @Override
    public void setEvent(Event event) {
        this.payload = event.getBody();
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // TODO Auto-generated method stub
    }

}
