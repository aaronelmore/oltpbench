package com.oltpbenchmark.benchmarks.ycsb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.mysql.jdbc.log.Log;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.ycsb.procedures.DeleteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.InsertRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadModifyWriteRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ReadRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.ScanRecord;
import com.oltpbenchmark.benchmarks.ycsb.procedures.UpdateRecord;
import com.oltpbenchmark.distributions.CounterGenerator;
import com.oltpbenchmark.distributions.IntegerGenerator;
import com.oltpbenchmark.distributions.UniformGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;

public class YCSBWorker extends Worker {

    private ZipfianGenerator readRecord;
    private static CounterGenerator insertRecord;
    private ZipfianGenerator randScan;
    private UniformGenerator onOff;
    private boolean stateOn = true;
    private int onSwitch = 101;
    private int coinFlip = 0;
    private final Map<Integer, String> m = new HashMap<Integer, String>();
    
    public YCSBWorker(int id, BenchmarkModule benchmarkModule, int init_record_count) {
        super(benchmarkModule, id);
        readRecord = new ZipfianGenerator(init_record_count);// pool for read keys
        randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
        onOff = new UniformGenerator(new Random(), 0, 100);
        synchronized (YCSBWorker.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
        } // SYNCH
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();
        /*
        
        coinFlip = onOff.nextInt();
        System.out.println(String.format("onSwitch:%s cointFlip:%s  stateOn:%s  swapping:%s",
                onSwitch, coinFlip, stateOn, (coinFlip> onSwitch)));
        if (coinFlip> onSwitch){
            System.out.println("Switch from : " + stateOn);
            stateOn = !stateOn;            
        }
        */
        if (stateOn){        
            if (procClass.equals(DeleteRecord.class)) {
                System.out.println("Delete");
                deleteRecord();
            } else if (procClass.equals(InsertRecord.class)) {
                System.out.println("Insert");
                insertRecord();
            } else if (procClass.equals(ReadModifyWriteRecord.class)) {
                System.out.println("RMW");
                readModifyWriteRecord();
            } else if (procClass.equals(ReadRecord.class)) {
                System.out.println("Read");
                readRecord();
            } else if (procClass.equals(ScanRecord.class)) {
                System.out.println("SCan");
                scanRecord();
            } else if (procClass.equals(UpdateRecord.class)) {
                System.out.println("Update");
                updateRecord();
            }
            conn.commit();
        } else {
            return (TransactionStatus.SUCCESS_PASS);
        }
        return (TransactionStatus.SUCCESS);
    }

    private void updateRecord() throws SQLException {
        UpdateRecord proc = this.getProcedure(UpdateRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        Map<Integer, String> values = buildValues(10);
        proc.run(conn, "USERTABLE", keyname, values);
    }

    private void scanRecord() throws SQLException {
        ScanRecord proc = this.getProcedure(ScanRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        int count = randScan.nextInt();
        proc.run(conn, keyname, count, new ArrayList<Map<Integer, String>>());
    }

    private void readRecord() throws SQLException {
        ReadRecord proc = this.getProcedure(ReadRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        proc.run(conn, "USERTABLE", keyname, new HashMap<Integer, String>());
    }

    private void readModifyWriteRecord() throws SQLException {
        ReadModifyWriteRecord proc = this.getProcedure(ReadModifyWriteRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        
        String fields[] = new String[10];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = TextGenerator.randomStr(rng(), 100);
        } // FOR
        
        this.m.clear();
        proc.run(conn, keyname, fields, this.m);
    }

    private void insertRecord() throws SQLException {
        InsertRecord proc = this.getProcedure(InsertRecord.class);
        assert (proc != null);
        int keyname = insertRecord.nextInt();
        // System.out.println("[Thread " + this.id+"] insert this:  "+ keyname);
        Map<Integer, String> values = buildValues(10);
        proc.run(conn, "USERTABLE", keyname, values);
    }

    private void deleteRecord() throws SQLException {
        DeleteRecord proc = this.getProcedure(DeleteRecord.class);
        assert (proc != null);
        int keyname = readRecord.nextInt();
        proc.run(conn, keyname);
    }

    private Map<Integer, String> buildValues(int numVals) {
        this.m.clear();
        for (int i = 1; i <= numVals; i++) {
            this.m.put(i, TextGenerator.randomStr(rng(), 100));
        }
        return this.m;
    }
}
