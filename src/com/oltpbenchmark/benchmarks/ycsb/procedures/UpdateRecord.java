package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateRecord extends Procedure {
    
    public final SQLStmt updateAllStmt = new SQLStmt(
        "UPDATE ? SET FIELD1=?,FIELD2=?,FIELD3=?,FIELD4=?,FIELD5=?," +
        "FIELD6=?,FIELD7=?,FIELD8=?,FIELD9=?,FIELD10=? WHERE YCSB_KEY=?"
    );
    
    public void run(Connection conn, String tablename, int keyname, Map<Integer,String> vals) throws SQLException {
    	PreparedStatement stmt = this.getPreparedStatement(conn, updateAllStmt);
		assert(vals.size()==10);
		stmt.setString(1, tablename);
		stmt.setInt(12,keyname); 
        for(Entry<Integer, String> s:vals.entrySet())
        {
        	stmt.setString(s.getKey()+1, s.getValue());
        }
        stmt.executeUpdate();
    }
}
