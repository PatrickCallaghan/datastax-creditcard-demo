package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.datastax.creditcard.model.BlacklistIssuer;
import com.datastax.creditcard.model.Transaction;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Dao class for all the blacklist tables.
 * @author patrickcallaghan
 *
 */
public class BlackListDao {

	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String blacklistIssuers = keyspaceName + ".blacklist_issuers";
	private static String blacklistCards = keyspaceName + ".blacklist_cards";
	private static String blacklistTransactions = keyspaceName + ".blacklist_transactions";

	private static final String GET_ALL_BLACKLIST_TRANSACTIONS = "select * from " + blacklistTransactions
			+ " where dummy = 'dummy'";
	private static final String GET_ALL_BLACKLIST_CARDS = "select * from " + blacklistCards;
	private static final String GET_ALL_BLACKLIST_ISSUERS = "select * from " + blacklistIssuers;
	private static final String INSERT_INTO_BLACKLIST_ISSUERS = "insert into " + blacklistIssuers
			+ "(issuer, city, amount) values (?,?,?);";
	private static final String INSERT_INTO_BLACKLIST_CARDS = "insert into " + blacklistCards
			+ "(dummy, cc_no, amount) values ('dummy',?,?);";

	private PreparedStatement insertBlacklistIssuers;
	private PreparedStatement insertBlacklistCards;
	private PreparedStatement getBlacklistIssuers;
	private PreparedStatement getBlacklistCards;
	private PreparedStatement getBlacklistTransactions;

	public BlackListDao(Session session) {
		this.session = session;
		
		this.insertBlacklistIssuers = session.prepare(INSERT_INTO_BLACKLIST_ISSUERS);
		this.insertBlacklistCards = session.prepare(INSERT_INTO_BLACKLIST_CARDS);
		this.getBlacklistIssuers = session.prepare(GET_ALL_BLACKLIST_ISSUERS);
		this.getBlacklistCards = session.prepare(GET_ALL_BLACKLIST_CARDS);
		this.getBlacklistTransactions = session.prepare(GET_ALL_BLACKLIST_TRANSACTIONS);
		
		this.getBlacklistTransactions.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
	}
	
	public void insertBlacklistIssuer(String issuer, String city, double amount) {

		session.execute(this.insertBlacklistIssuers.bind(issuer, city, amount));
	}

	public void insertBlacklistCard(String cc_no, double amount) {

		session.execute(this.insertBlacklistCards.bind(cc_no, amount));
	}

	public Map<String, Double> getBlacklistCards() {

		ResultSet rs = this.session.execute(this.getBlacklistCards.bind());
		Iterator<Row> iter = rs.iterator();

		Map<String, Double> blacklistCards = new HashMap<String, Double>();

		while (iter.hasNext()) {

			Row row = iter.next();
			blacklistCards.put(row.getString("cc_no"), row.getDouble("amount"));
		}

		return blacklistCards;
	}
	
	public List<BlacklistIssuer> getBlacklistIssuers() {

		ResultSet rs = this.session.execute(this.getBlacklistIssuers.bind());
		Iterator<Row> iter = rs.iterator();

		Map<String, BlacklistIssuer> blacklistIssuers = new HashMap<String, BlacklistIssuer>();

		BlacklistIssuer blacklistIssuer = new BlacklistIssuer();
		Map<String, Double> map = new HashMap<String, Double>();

		while (iter.hasNext()) {

			Row row = iter.next();
			String issuer = row.getString("issuer");

			if (blacklistIssuers.containsKey(issuer)) {
				map = blacklistIssuers.get(issuer).getCityAmount();
			}

			map.put(row.getString("city"), row.getDouble("amount"));
			blacklistIssuer.setCityAmount(map);

			blacklistIssuers.put(issuer, blacklistIssuer);
		}

		return new ArrayList<BlacklistIssuer>(blacklistIssuers.values());
	}

	public List<Transaction> getBlacklistTransactions() {
		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet rs = this.session.execute(this.getBlacklistTransactions.bind());
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {

			Row row = iter.next();
			transactions.add(rowToTransaction(row));
		}

		return transactions;
	}	

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();
		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setIssuer(row.getString("issuer"));
		t.setItems(row.getMap("items", String.class, Double.class));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getUUID("transaction_id").toString());
		t.setTransactionTime(row.getDate("date"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));

		return t;
	}
}
