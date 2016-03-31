package org.apache.bigtop.bigpetstore.flink.scala

import com.github.rnowling.bps.datagenerator.datamodels.Transaction
import org.apache.bigtop.bigpetstore.flink.java.FlinkTransaction

package object generator {
  implicit def transactionToFlink(transaction: Transaction) : FlinkTransaction = new FlinkTransaction(transaction)
}