package com.datastax.creditcard.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.UserRule;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Dao class for all the blacklist tables.
 * @author patrickcallaghan
 *
 */
public class UserRulesDao {

	private Session session;

	private static Logger logger = LoggerFactory.getLogger(UserRulesDao.class);
	private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd");
	
	private static String keyspaceName = "datastax_creditcard_demo";
	private static String userRules = keyspaceName + ".user_rules";

	private static final String GET_ALL_USER_RULES = "select * from " + userRules
			+ " where user_id = ?";
	private static final String GET_USER_RULE = "select * from " + userRules
			+ " where user_id = ? and rule_id = ?";

	private static final String INSERT_USER_RULE = "insert into " +userRules+ " (user_id, rule_id, rule_name, merchant, amount, nooftransactions, noofdays) values (?,?,?,?,?,?,?)";
	private static final String DELETE_USER_RULE = "delete from " +userRules+ " where user_id = ? and rule_id = ?";
			
	private PreparedStatement getAllUserRules;
	private PreparedStatement getUserRule;
	private PreparedStatement insertUserRule;
	private PreparedStatement deleteUserRule;

	public UserRulesDao(Session session) {
		this.session = session;
		
		this.getAllUserRules = session.prepare(GET_ALL_USER_RULES);
		this.getUserRule = session.prepare(GET_USER_RULE);
		this.insertUserRule = session.prepare(INSERT_USER_RULE);
		this.deleteUserRule = session.prepare(DELETE_USER_RULE);
	}
	
	public void insertUserRule(UserRule userRule){
		
		BoundStatement bound = new BoundStatement(this.insertUserRule);
		
		session.execute(bound.bind(userRule.getUserId(), userRule.getRuleId(), userRule.getRuleName(), 
							userRule.getMerchant(), userRule.getAmount(), userRule.getNoOfTransactions(), userRule.getNoOfDays()));
		
		return;
	}
	
	public List<UserRule> getAllUserRules(String userId) {

		ResultSet rs = this.session.execute(this.getAllUserRules.bind(userId));
		Iterator<Row> iter = rs.iterator();

		List<UserRule> userRules = new ArrayList<UserRule>();

		while (iter.hasNext()) {

			Row row = iter.next();
			UserRule userRule = rowToUserRule(row);		
			userRules.add(userRule);
		}

		return userRules;
	}

	public UserRule getUserRule(String userId, String ruleId) {
		ResultSet rs = this.session.execute(this.getUserRule.bind(userId, ruleId));
		if(!rs.isExhausted()){
			return rowToUserRule(rs.one());
		}else{
			return null;
		}
	}
	

	public void deleteUserRule(String userId, String ruleId) {
		BoundStatement bound = new BoundStatement(this.deleteUserRule);
		session.execute(bound.bind(userId, ruleId));		
	}
	
	private UserRule rowToUserRule(Row row) {
		UserRule userRule = new UserRule();
		userRule.setUserId(row.getString("user_id"));
		userRule.setRuleId(row.getString("rule_id"));
		userRule.setRuleName(row.getString("rule_name"));
		userRule.setMerchant(row.getString("merchant"));
		userRule.setAmount(row.getDouble("amount"));
		userRule.setNoOfDays(row.getInt("noOfDays"));
		userRule.setNoOfTransactions(row.getInt("noOfTransactions"));
		return userRule;
	}
	
	
		
	private String formatDate(Date date){
		return this.dateFormatter.format(date);
	}

}
