package com.datastax.creditcard.rules;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.BlacklistMerchant;
import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;

public class MerchantBlackListRule extends AbstractRule {

	private static Logger logger = LoggerFactory.getLogger(MerchantBlackListRule.class);
	
	@Override
	public Status processRule(Transaction transaction) {
		if (this.issuerBlackList.containsKey(transaction.getMerchant())) {

			logger.info("Processing Merchant Blacklist rule");
			
			BlacklistMerchant blacklistIssuer = this.issuerBlackList.get(transaction.getMerchant());

			Map<String, Double> cityAmount = blacklistIssuer.getCityAmount();

			if (cityAmount.containsKey(transaction.getLocation())) {
				double checkAmount = cityAmount.get(transaction.getLocation());

				if (checkAmount < transaction.getAmount()) {
					transaction.setStatus(Status.CHECK.toString());
					transaction.setNotes("Amount is " + transaction.getAmount() + " when check limit has been set to "
							+ checkAmount + ".\n" + transaction.getNotes());

					logger.info("Transaction :" + transaction.getTransactionId() + " needs checking");
					
					return Status.CHECK;
				}
			}
		}
		
		return Status.APPROVED;
	}
}
