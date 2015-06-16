package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.datastax.creditcard.model.Transaction;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
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
	private DateFormat dateTimeFormatter = new SimpleDateFormat("yyyyMMdd-hh:mm:ss");

	private PreparedStatement updateIssuerDateMinTableStmt;
	private PreparedStatement updateUserDateMinTableStmt;
	private PreparedStatement updateTableStmt;

	private PreparedStatement getCountTableStmt;
	private PreparedStatement getIssuerCountTableStmt;
	private PreparedStatement getUserCountTableStmt;
	private PreparedStatement getIssuerHourCountTableStmt;
	private PreparedStatement getUserHourCountTableStmt;
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String counterIssuerDateMinTable = keyspaceName + ".trans_issuer_date_minute_counter";
	private static String counterUserDateMinTable = keyspaceName + ".trans_user_date_minute_counter";
	private static String counterTable = keyspaceName + ".trans_counter";
	
	private static String updateIssuerDateMinTable = "update " + counterIssuerDateMinTable + " set total = total + 1 where "
			+ "issuer = ? and date = ? and hour = ? and minute = ?";
	private static String updateUserDateMinTable = "update " + counterUserDateMinTable + " set total = total + 1 where "
			+ "user = ? and date = ? and hour = ? and minute = ?";
	private static String updateTable = "update " + counterTable + " set total = total + 1 where date = ?";		
	
	private static String getCountTable = "select date, total from  " + counterTable + " where date = ?";	
	private static String getIssuerCountTable = "select date, total from  " + counterIssuerDateMinTable + " where issuer = ? and date = ?";	
	private static String getIssuerHourCountTable = "select date, hour, minute, total from  " + counterIssuerDateMinTable + " where issuer = ? and date = ? and hour = ?";	
	private static String getUserCountTable = "select date, total from  " + counterUserDateMinTable + " where user = ? and date = ?";
	private static String getUserHourCountTable = "select date, hour, minute, total from  " + counterUserDateMinTable + " where user = ? and date = ? and hour = ?";
	
	public CounterDao(Session session) {
		this.session = session;
		
		this.updateIssuerDateMinTableStmt = this.session.prepare(updateIssuerDateMinTable);
		this.updateUserDateMinTableStmt = this.session.prepare(updateUserDateMinTable);
		this.updateTableStmt = this.session.prepare(updateTable);
		
		this.getCountTableStmt = this.session.prepare(getCountTable);
		this.getIssuerCountTableStmt = this.session.prepare(getIssuerCountTable);
		this.getIssuerHourCountTableStmt = this.session.prepare(getIssuerHourCountTable);
		this.getUserCountTableStmt = this.session.prepare(getUserCountTable);
		this.getUserHourCountTableStmt = this.session.prepare(getUserHourCountTable);		
	}
	
	public void updateCounters(Transaction transaction, String userId){
		
		String dateStr = dateFormatter.format(transaction.getTransactionTime());
		DateTime date = new DateTime(transaction.getTransactionTime());
		
		BatchStatement batch = new BatchStatement(Type.COUNTER);
		batch.add(this.updateTableStmt.bind(dateStr));		
		batch.add(this.updateIssuerDateMinTableStmt.bind(transaction.getIssuer(), dateStr, date.getHourOfDay(), date.getMinuteOfHour()));		
		batch.add(this.updateUserDateMinTableStmt.bind(userId, dateStr, date.getHourOfDay(), date.getMinuteOfHour()));
		this.session.execute(batch);
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
	
	public Map<String, Integer> getIssuerCounterLastNDays(String issuer, DateTime dateTime, int nDays){
		
		BoundStatement bound = new BoundStatement(this.getIssuerCountTableStmt);
		return this.getCounterLastNDays(bound, issuer, dateTime, nDays);		
	}	
	public Map<String, Integer> getUserCounterLastNDays(String user, DateTime dateTime, int nDays){
		BoundStatement bound = new BoundStatement(this.getUserCountTableStmt);
		return this.getCounterLastNDays(bound, user, dateTime, nDays);
	}
	
	private Map<String, Integer> getCounterLastNDays(BoundStatement bound, String type, DateTime dateTime, int nDays){
		
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();		
		Map<String, Integer> dateCountMap = new HashMap<String, Integer>();
				
		for (int i = 0 ; i < nDays; i++){
			
			dateTime.minusDays(i);
			String date = dateFormatter.format(dateTime.toDate());
		
			futures.add(this.session.executeAsync(bound.bind(type, date)));			
		}
				
		for (ResultSetFuture future : futures) {
			
		    ResultSet rs = future.getUninterruptibly();
		    Row row = rs.one();
		    dateCountMap.put(row.getString("date"), row.getInt("total"));
		}
		
		return dateCountMap;		
	}
	
	public Map<String, Integer> getIssuerHourCounterLastNDays(String issuer, DateTime dateTime, int hour, int nDays){
		
		BoundStatement bound = new BoundStatement(this.getIssuerHourCountTableStmt);
		return this.getHourCounterLastNDays(bound, issuer, dateTime, hour, nDays);		
	}	
	public Map<String, Integer> getUserHourCounterLastNDays(String user, DateTime dateTime, int hour, int nDays){
		BoundStatement bound = new BoundStatement(this.getUserHourCountTableStmt);
		return this.getHourCounterLastNDays(bound, user, dateTime, hour, nDays);
	}
	
		
	private Map<String, Integer> getHourCounterLastNDays(BoundStatement bound, 
			String type, DateTime dateTime, int hour, int nDays){
		
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();		
		Map<String, Integer> dateCountMap = new HashMap<String, Integer>();
				
		for (int i = 0 ; i < nDays; i++){
			
			dateTime.minusDays(i);
			String date = dateFormatter.format(dateTime.toDate());
		
			futures.add(this.session.executeAsync(bound.bind(type, date, hour)));			
		}
				
		for (ResultSetFuture future : futures) {
			
		    ResultSet rs = future.getUninterruptibly();
		    Row row = rs.one();
		    dateCountMap.put(row.getString("date") + "-" + row.getInt("hour")  + "-" + row.getInt("minute"), row.getInt("total"));
		}
		
		return dateCountMap;		
	}
	
	public static void main(String[] args) {
	
	}

}
