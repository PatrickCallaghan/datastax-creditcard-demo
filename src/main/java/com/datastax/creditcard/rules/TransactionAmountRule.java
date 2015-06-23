package com.datastax.creditcard.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;

public class TransactionAmountRule extends AbstractRule {

	private static Logger logger = LoggerFactory.getLogger(TransactionAmountRule.class);
	
	@Override
	public Status processRule(Transaction transaction) {
		if (this.ccNoBlackMap.containsKey(transaction.getCreditCardNo())) {
			double checkAmount = ccNoBlackMap.get(transaction.getCreditCardNo());

			if (checkAmount < transaction.getAmount()) {
				transaction.setStatus(Status.CHECK.toString());
				transaction.setNotes("Amount is " + transaction.getAmount() + " when check limit has been set to "
						+ checkAmount + ".\n" + transaction.getNotes());

				logger.info("Transaction :" + transaction.getTransactionId() + " needs checking");
			}
			return Status.CHECK;
		} else if (transaction.getAmount() > 4500) {
			transaction.setStatus(Status.CLIENT_APPROVAL.toString());
			transaction.setNotes("Amount is " + transaction.getAmount());

			logger.info("Transaction :" + transaction.getTransactionId() + " needs client Approval");
			return Status.CLIENT_APPROVAL;
		}
		
		return Status.APPROVED;
	}

}
