package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Dao class for all the counter tables.
 * @author patrickcallaghan
 *
 */
public class CounterDao {

	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");

	private PreparedStatement updateIssuerDateMinTableStmt;
	private PreparedStatement updateUserDateMinTableStmt;
	private PreparedStatement updateTableStmt;

	private PreparedStatement getCountTableStmt;
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String counterIssuerDateMinTable = keyspaceName + ".trans_issuer_date_minute_counter";
	private static String counterUserDateMinTable = keyspaceName + ".trans_user_date_minute_counter";
	private static String counterTable = keyspaceName + ".trans_counter";
	
	private static String updateIssuerDateMinTable = "update " + counterIssuerDateMinTable + " set total = total + 1 where "
			+ "issuer = ? and date = ? and hour = ? and min = ? with ttl 860000";

	private static String updateUserDateMinTable = "update " + counterUserDateMinTable + " set total = total + 1 where "
			+ "user = ? and date = ? and hour = ? and min = ? with ttl 864000";

	private static String updateTable = "update " + counterTable + " set total = total + 1 where date = ?";
	
	
	private static String getCountTable = "select date, total from  " + counterTable + " where date = ?";

	public CounterDao(Session session) {
		this.session = session;
		
		this.updateIssuerDateMinTableStmt = this.session.prepare(updateIssuerDateMinTable);
		this.updateUserDateMinTableStmt = this.session.prepare(updateUserDateMinTable);
		this.updateTableStmt = this.session.prepare(updateTable);
		
		this.getCountTableStmt = this.session.prepare(getCountTable);
	}
	
	public void updateIssuerCounter(String issuer, DateTime dateTime){
		String date = dateFormatter.format(dateTime.toDate());
		int hour = dateTime.getHourOfDay();
		int min = dateTime.getMinuteOfHour();
		
		BoundStatement bound = new BoundStatement(this.updateIssuerDateMinTableStmt);
		this.session.execute(bound.bind(issuer, date, hour, min));
	}
	
	public void updateUserCounter(String user, DateTime dateTime){
		String date = dateFormatter.format(dateTime.toDate());
		int hour = dateTime.getHourOfDay();
		int min = dateTime.getMinuteOfHour();
		
		BoundStatement bound = new BoundStatement(this.updateUserDateMinTableStmt);
		this.session.execute(bound.bind(user, date, hour, min));
	}
	
	public void updateTransCounter(DateTime dateTime){
		String date = dateFormatter.format(dateTime.toDate());
		
		BoundStatement bound = new BoundStatement(this.updateTableStmt);
		this.session.execute(bound.bind(date));		
	}
	
	public Map<String, Integer> getTransCounterLastNDays(DateTime dateTime, int nDays){
		
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();		
		Map<String, Integer> dateCountMap = new HashMap<String, Integer>();
				
		for (int i = 0 ; i < nDays; i++){
			BoundStatement bound = new BoundStatement(this.getCountTableStmt);
			
			dateTime.minusDays(i);
			String date = dateFormatter.format(dateTime.toDate());
		
			futures.add(this.session.executeAsync(bound.bind(date)));			
		}
				
		for (ResultSetFuture future : futures) {
			
		    ResultSet rs = future.getUninterruptibly();
		    Row row = rs.one();
		    dateCountMap.put(row.getString("date"), row.getInt("total"));
		}
		
		return dateCountMap;
		
	}
	
	public static void main(String[] args) {
	
	}

}
