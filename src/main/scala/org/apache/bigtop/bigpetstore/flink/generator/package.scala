package org.apache.bigtop.bigpetstore.flink

import com.github.rnowling.bps.datagenerator.datamodels.Transaction

package object generator {
  implicit def transactionToFlink(transaction: Transaction) : FlinkTransaction = new FlinkTransaction(transaction)
}