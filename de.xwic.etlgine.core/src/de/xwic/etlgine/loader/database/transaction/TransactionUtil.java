package de.xwic.etlgine.loader.database.transaction;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class TransactionUtil {

	public static DefaultTransactionDefinition getDefaultTransactionDefinition() {
		DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
		transactionDefinition.setName("de.xwic.etlgine.loader.database.DatabaseLoader-mainTransaction");
		transactionDefinition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
		
		return transactionDefinition;
	}
}
