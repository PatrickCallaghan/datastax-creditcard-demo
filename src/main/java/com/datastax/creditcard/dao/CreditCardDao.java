package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.CreditBalance;
import com.datastax.creditcard.model.Merchant;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.User;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;

public class CreditCardDao {

	private static Logger logger = LoggerFactory.getLogger(CreditCardDao.class);
	private static final int DEFAULT_LIMIT = 10000;
	private Session session;

	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String balanceTable = keyspaceName + ".credit_card_transactions_balance";
	private static String transactionsByMerchantTable = keyspaceName + ".credit_card_transactions_by_merchant_date";

	private static String transactionTable = keyspaceName + ".transactions";
	private static String latestTransactionTable = keyspaceName + ".latest_transactions";
	private static String userTable = keyspaceName + ".users";
	private static String issuerTable = keyspaceName + ".issuers";

	private static final String GET_TRANSACTIONS_CCNO_AND_ID = "select transaction_id from " + balanceTable
			+ " where cc_no = ?";
	private static final String INSERT_INTO_TRANSACTION = "Insert into " + transactionTable
			+ " (cc_no, transaction_time, transaction_id, items, location, issuer, amount, user_id, status, notes) values (?,?,?,?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_LATEST_TRANSACTION = "Insert into " + latestTransactionTable
			+ " (cc_no, transaction_time, transaction_id, items, location, issuer, amount, user_id, status, notes) values (?,?,?,?,?,?,?,?,?,?) using ttl 864000";	
	private static final String INSERT_INTO_MERCHANT = "insert into " + transactionsByMerchantTable
			+ "(issuer, date, transaction_id, cc_no, transaction_time, amount) values (?,?,?,?,?,?)";
	private static final String UPDATE_BALANCE = "update " + balanceTable
			+ " set balance = ?, balance_at = ?  where cc_no = ?";

	private static final String UPDATE_TRANSACTION_STATUS = "update " + transactionTable
			+ " set status = ?, notes = ? where transaction_id = ?";
	private static final String UPDATE_LATEST_TRANSACTION_STATUS = "update " + latestTransactionTable
			+ " set status = ?, notes = ? where cc_no = ? and transaction_time = ?";
	private static final String INSERT_BALANCE = "insert into " + balanceTable
			+ "(cc_no, balance, balance_at) values (?,?,?)";

	private static final String GET_ALL_MERCHANT_BY_DATE = "select transaction_id,amount,cc_no   from " + transactionsByMerchantTable
			+ " where issuer = ? and date = ?";
	private static final String GET_ALL_TRANSACTIONS_BY_TIME = "select transaction_id,amount,cc_no from "
			+ balanceTable + " where cc_no = ? and transaction_id > ? and transaction_id < ? LIMIT ?";
	private static final String GET_TRANSACTIONS_BY_ID = "select * from "
			+ transactionTable + " where transaction_id = ?";
	private static final String GET_TRANSACTIONS_BY_CCNO = "select * from "
			+ latestTransactionTable + " where cc_no = ? order by transaction_time desc";

	private static final String GET_ALL_CREDIT_CARDS = "select cc_no, balance_at, balance from " + balanceTable;
	private static final String GET_USER = "Select * from " + userTable + " where user_id = ?";
	private static final String GET_MERCHANT = "Select * from " + issuerTable + " where id = ?";
	
	private PreparedStatement insertTransactionStmt;
	private PreparedStatement insertLatestTransactionStmt;
	private PreparedStatement insertMerchantStmt;
	private PreparedStatement updateBalance;
	private PreparedStatement insertBalance;
	private PreparedStatement getTransTime;
	private PreparedStatement getUser;
	private PreparedStatement getMerchant;
	private PreparedStatement getMerchantTransactionsByDate;
	private PreparedStatement updateTransactionStatus;
	private PreparedStatement getTransaction;
	private PreparedStatement getTransactionById;
	private PreparedStatement updateLatestTransactionStatus;
	private PreparedStatement getTransactionByCCno;
	
	private CounterDao counterDao;
	private BlackListDao blackListDao;
	private UserRulesDao userRulesDao;
	

	public CreditCardDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()				
				.addContactPoints(contactPoints)
				.build();

		this.session = cluster.connect();
		
		counterDao = new CounterDao(session);
		blackListDao = new BlackListDao(session);
		userRulesDao = new UserRulesDao(session);

		try {
			this.insertTransactionStmt = session.prepare(INSERT_INTO_TRANSACTION);
			this.insertLatestTransactionStmt = session.prepare(INSERT_INTO_LATEST_TRANSACTION);
			this.insertMerchantStmt = session.prepare(INSERT_INTO_MERCHANT);

			this.updateBalance = session.prepare(UPDATE_BALANCE);
			this.insertBalance = session.prepare(INSERT_BALANCE);
			this.getTransTime = session.prepare(GET_ALL_TRANSACTIONS_BY_TIME);
			this.getUser = session.prepare(GET_USER);
			this.getMerchant = session.prepare(GET_MERCHANT);
			this.getMerchantTransactionsByDate = session.prepare(GET_ALL_MERCHANT_BY_DATE);


			this.updateTransactionStatus = session.prepare(UPDATE_TRANSACTION_STATUS);
			this.updateLatestTransactionStatus = session.prepare(UPDATE_LATEST_TRANSACTION_STATUS);
			this.getTransaction = session.prepare(GET_TRANSACTIONS_CCNO_AND_ID);
			this.getTransactionById = session.prepare(GET_TRANSACTIONS_BY_ID);
			this.getTransactionByCCno = session.prepare(GET_TRANSACTIONS_BY_CCNO);

			this.insertLatestTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertTransactionStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.insertMerchantStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			this.updateBalance.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getTransTime.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getUser.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			this.getMerchantTransactionsByDate.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
		} catch (Exception e) {
			e.printStackTrace();
			session.close();
			cluster.close();
		}
	}

	public void saveTransaction(Transaction transaction) {
		insertTransaction(transaction);
	}
	
	public void createCreditCards(int cards) {
		for (int i = 1; i < cards + 1; i++) {
			session.execute(this.insertBalance.bind("" + i, 0d, DateTime.parse("2001-01-01").toDate()));
		}
	}

	public void updateStatusNotes(String status, String notes, String transactionId) {

		Transaction transaction = this.getTransaction(transactionId);

		BatchStatement batch = new BatchStatement(Type.LOGGED);
		batch.add(updateTransactionStatus.bind(status, notes, transaction.getTransactionId()));
		batch.add(updateLatestTransactionStatus.bind(status, notes, transaction.getCreditCardNo(),
				transaction.getTransactionTime()));
		session.execute(batch);
	}

	public void insertTransaction(Transaction transaction) {

		DateTime dateTime = new DateTime(transaction.getTransactionTime());
		String date = dateFormatter.format(dateTime.toDate());

		BatchStatement batch = new BatchStatement(Type.LOGGED);

		batch.add(this.insertTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getItems(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(), transaction.getNotes()));
		batch.add(this.insertLatestTransactionStmt.bind(transaction.getCreditCardNo(), transaction.getTransactionTime(),
				transaction.getTransactionId(), transaction.getItems(), transaction.getLocation(),
				transaction.getMerchant(), transaction.getAmount(), transaction.getUserId(), transaction.getStatus(), transaction.getNotes()));
		batch.add(this.insertMerchantStmt.bind(transaction.getMerchant(), date, transaction.getTransactionId(),
				transaction.getCreditCardNo(), transaction.getTransactionTime(), transaction.getAmount()));
		
		if (!transaction.getStatus().equals(Transaction.Status.APPROVED.toString())){
			this.blackListDao.addInsertBlacklistTransactionToBatch(batch, transaction.getTransactionTime(), transaction);
		}

		session.execute(batch);
	}

	public User getUser(String userId) {

		ResultSet rs = this.session.execute(this.getUser.bind(userId));
		Row row = rs.one();

		return rowToUser(row);
	}

	private User rowToUser(Row row) {
		User user = new User();
		user.setUserId(row.getString("user_id"));
		user.setCityName(row.getString("city"));
		user.setCreditCardNo(row.getString("cc_no"));
		user.setFirstname(row.getString("first"));
		user.setGender(row.getString("gender"));
		user.setLastname(row.getString("last"));
		user.setStateName(row.getString("state"));
	
		return user;
	}

	public boolean updateCreditCardWithBalance() {
		// Get all credit cards
		ResultSet resultSetAllCreditCards = session.execute(GET_ALL_CREDIT_CARDS);
		List<CreditBalance> creditBalances = this.createCreditBalances(resultSetAllCreditCards);
		Date balanceAt;

		logger.info("Updating balances for " + creditBalances.size() + " credit cards.");

		for (CreditBalance creditBalance : creditBalances) {

			balanceAt = creditBalance.getBalanceAt() == null ? DateTime.parse("2001-01-01").toDate() : creditBalance
					.getBalanceAt();
			double total = creditBalance.getAmount() == null ? 0 : creditBalance.getAmount();

			// set the new balance date to the last one until we get new
			// transactions.
			Date newBalanceAt = balanceAt;
			ResultSet resultSet = session.execute(this.getTransTime.bind(creditBalance.getCreditCardNo(), balanceAt));
			Iterator<Row> iterator = resultSet.iterator();

			while (iterator.hasNext()) {
				Row row = iterator.next();

				total = total + row.getDouble("amount");

				if (row.getDate("transaction_time").after(newBalanceAt)) {
					newBalanceAt = row.getDate("transaction_time");
				}
			}

			// update the balance with the date of the latest transaction that
			// has been processed.
			session.execute(updateBalance.bind(total, newBalanceAt, creditBalance.getCreditCardNo()));
		}

		return true;
	}

	private List<CreditBalance> createCreditBalances(ResultSet resultSet) {

		List<CreditBalance> creditBalances = new ArrayList<CreditBalance>();
		Iterator<Row> iterator = resultSet.iterator();

		while (iterator.hasNext()) {
			Row row = iterator.next();

			creditBalances.add(new CreditBalance(row.getString("cc_no"), row.getDate("balance_at"), row
					.getDouble("balance")));
		}
		return creditBalances;
	}

	public List<Transaction> getAllTransactions(String creditCardNo, long start, long end, int limit) {
		
		if (start < 0)
			start = 0;
		if (end < 0)
			end = new Date().getTime();
		if (limit < 0)
			limit = DEFAULT_LIMIT;

		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet rs = this.session.execute(this.getTransTime.bind(creditCardNo, new Date(start), new Date(end), limit));
		
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {

			Row row = iter.next();
			transactions.add(rowToTransaction(row));
		}

		return transactions;
	}

	public Transaction getTransaction(String transactionId) {

		ResultSet rs = this.session.execute(this.getTransactionById.bind(transactionId));
		
		Row row = rs.one();
		if (row == null){
			throw new RuntimeException("Error - no transaction for id:" + transactionId);			
		}

		return rowToTransaction(row);		
	}

	private Transaction rowToTransaction(Row row) {

		Transaction t = new Transaction();
		
		t.setAmount(row.getDouble("amount"));
		t.setCreditCardNo(row.getString("cc_no"));
		t.setMerchant(row.getString("merchant"));
		t.setItems(row.getMap("items", String.class, Double.class));
		t.setLocation(row.getString("location"));
		t.setTransactionId(row.getString("transaction_id"));
		t.setTransactionTime(row.getDate("transaction_time"));
		t.setUserId(row.getString("user_id"));
		t.setNotes(row.getString("notes"));
		t.setStatus(row.getString("status"));

		return t;
	}

	public List<Transaction> getMerchantTransactions(String issuer, long date) {

		List<Transaction> transactions = new ArrayList<Transaction>();
		ResultSet rs = this.session.execute(this.getMerchantTransactionsByDate.bind(issuer,
				dateFormatter.format(new Date(date))));
		Iterator<Row> iter = rs.iterator();

		while (iter.hasNext()) {

			Row row = iter.next();
			transactions.add(rowToTransaction(row));
		}

		return transactions;
	}
	
	public Transaction getTransactions(String transactionId) {

		logger.info("Getting transaction :" + transactionId);
		
		ResultSet resultSet = this.session.execute(getTransactionById.bind(transactionId));		
		Row row = resultSet.one();

		if (row == null){
			throw new RuntimeException("Error - no issuer for id:" + transactionId);			
		}
		return rowToTransaction(row);
	}
	
	
	public List<Transaction> getLatestTransactionsForCCNo(String ccNo) {		
		ResultSet resultSet = this.session.execute(getTransactionByCCno.bind(ccNo));		
		List<Row> rows = resultSet.all();
		
		List<Transaction> transactions = new ArrayList<Transaction>();

		for (Row row : rows){
			transactions.add(rowToTransaction(row));
		}
		
		return transactions;
	}
	
	public List<Transaction> getTransactions(List<String> transactionIds) {

		List<Transaction> transactions = new ArrayList<Transaction>();
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		for (String transactionId : transactionIds) {

			results.add(this.session.executeAsync(getTransactionById.bind(transactionId)));
		}

		for (ResultSetFuture future : results) {
			transactions.add(this.rowToTransaction(future.getUninterruptibly().one()));
		}

		return transactions;
	}


	public Map<String,String> getCreditCardUserIdMap() {
		
		Statement stmt = new SimpleStatement ("select cc_no, user_id from " + userTable);
		stmt.setFetchSize(DEFAULT_LIMIT);
		ResultSet rs = this.session.execute(stmt);		
		Iterator<Row> iter = rs.iterator();
		
		Map<String,String> ccNoUserIdMap = new HashMap<String, String>();
		
		while (iter.hasNext()){
			
			Row row = iter.next();
			ccNoUserIdMap.put(row.getString("cc_no"), row.getString("user_id"));
		}
		
		return ccNoUserIdMap;
	}
	
	public List<String> getCreditCardNosIter() {
		
		Statement stmt = new SimpleStatement ("select * from " + userTable);
		ResultSet rs = this.session.execute(stmt);		
		Iterator<Row> iter = rs.iterator();
		
		List<String> creditCardNos = new ArrayList<String>();
		
		int count=0;
		
		while (iter.hasNext()){
			
			Row row = iter.next();
			creditCardNos.add(row.getString("cc_no"));
			count++;
		}
		
		logger.info("Count (iter): " + count);
		return creditCardNos;
	}

	public CounterDao getCounterDao(){
		return this.counterDao;
	}
	
	public BlackListDao getBlackListDao(){
		return this.blackListDao;
	}
	
	public UserRulesDao getUserRulesDao() {
		return userRulesDao;
	}
	
	public Merchant getMerchant(String issuerId) {
		ResultSet rs = this.session.execute(this.getMerchant.bind(issuerId));
		Row row = rs.one();

		if (row == null){
			throw new RuntimeException("Error - no merchant for id:" + issuerId);			
		}

		return rowToMerchant(row);
	}

	private Merchant rowToMerchant(Row row) {
		return new Merchant(row.getString("id"), row.getString("name"), row.getString("location"));
	}

}
