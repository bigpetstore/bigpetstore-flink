package org.apache.bigtop.bigpetstore.flink

import com.github.rnowling.bps.datagenerator.datamodels.Transaction
import org.apache.flink.examples.java.bigpetstore.FlinkTransaction

package object generator {
  implicit def transactionToFlink(transaction: Transaction) : FlinkTransaction = new FlinkTransaction(transaction)
}