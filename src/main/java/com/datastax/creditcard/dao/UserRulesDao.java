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

	private PreparedStatement getAllUserRules;

	public UserRulesDao(Session session) {
		this.session = session;
		
		this.getAllUserRules = session.prepare(GET_ALL_USER_RULES);
	}
	
	
	public List<UserRule> getAllUserRules(String userId) {

		ResultSet rs = this.session.execute(this.getAllUserRules.bind(userId));
		Iterator<Row> iter = rs.iterator();

		List<UserRule> userRules = new ArrayList<UserRule>();

		while (iter.hasNext()) {

			Row row = iter.next();
			UserRule userRule = new UserRule();
			userRule.setUserId(row.getString("user_id"));
			userRule.setRuleId(row.getString("rule_id"));
			userRule.setRuleName(row.getString("rule_name"));
			userRule.setIssuer(row.getString("issuer"));
			userRule.setAmount(row.getDouble("amount"));
			userRule.setNoOfDays(row.getInt("noOfDays"));
			userRule.setNoOfTransactions(row.getInt("noOfTransactions"));
		
			userRules.add(userRule);
		}

		return userRules;
	}
		
	private String formatDate(Date date){
		return this.dateFormatter.format(date);
	}
}
