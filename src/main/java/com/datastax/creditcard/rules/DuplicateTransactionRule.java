package com.datastax.creditcard.rules;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.creditcard.model.Transaction;
import com.datastax.creditcard.model.Transaction.Status;

public class DuplicateTransactionRule extends AbstractRule {

	private static final long ONE_MINUTE = 60*1000;
	private static Logger logger = LoggerFactory.getLogger(DuplicateTransactionRule.class);
	
	@Override
	public Status processRule(Transaction transaction) {
				
		Transaction lastTransaction = this.getLastTransaction();

		if (almostEquals (transaction, lastTransaction)){
			transaction.setStatus(Status.CLIENT_APPROVAL.toString());
			transaction.setNotes("Duplicate Transaction noticed - Same transaction as " + lastTransaction.getTransactionId());

			logger.info("Transaction :" + transaction.getTransactionId() + " needs client Approval");
			return Status.CLIENT_APPROVAL;			
		}
		
		return Status.APPROVED;
	}

	private boolean almostEquals(Transaction transaction, Transaction lastTransaction) {
		if (lastTransaction == null) {
			return false;
		}

		if ((transaction.getAmount().doubleValue() == lastTransaction.getAmount().doubleValue())
				&& transaction.getIssuer().equals(lastTransaction.getIssuer())
				&& transaction.getLocation().equals(lastTransaction.getLocation())
				&& transaction.getCreditCardNo().equals(lastTransaction.getCreditCardNo())
				&& (transaction.getTransactionTime().getTime() - ONE_MINUTE) < lastTransaction.getTransactionTime().getTime()){
			return true;
		}
		return false;
	}
}
