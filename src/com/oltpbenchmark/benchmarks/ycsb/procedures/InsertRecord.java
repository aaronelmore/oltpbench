package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class InsertRecord extends Procedure{
    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO ? VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, String tablename, int keyname, Map<Integer,String> vals) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, insertStmt);
        stmt.setString(1, tablename);
        stmt.setInt(2, keyname);
        for(Entry<Integer,String> s:vals.entrySet())
        {
        	stmt.setString(s.getKey()+2, s.getValue());
        }            
        stmt.executeUpdate();
    }

}
