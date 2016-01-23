/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bigtop.bigpetstore.flink;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rnowling.bps.datagenerator.CustomerGenerator;
import com.github.rnowling.bps.datagenerator.DataLoader;
import com.github.rnowling.bps.datagenerator.PurchasingProfileGenerator;
import com.github.rnowling.bps.datagenerator.StoreGenerator;
import com.github.rnowling.bps.datagenerator.TransactionGenerator;
import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.PurchasingProfile;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
@SuppressWarnings("serial")
public class FlinkBPSGenerator {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final double BURNIN_TIME = 7.0;

  /**
   * Args:
   *  (1) the output file (i.e. /tmp/a => write data files to /tmp/a/1, /tmp/a/2, and so on).
   *  (2) sim length (i.e. 10 => 10 days).
   *  (3) the stores (i.e.  3 => 3 stores randomly created)
   */
  public static void main(String[] args) throws Exception {

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String inputFile = parameterTool.get("inputFile");
    final int simulationLength = Integer.parseInt(parameterTool.get("inSimLength"));
    final int nStores = Integer.parseInt(parameterTool.get("inStores"));
    final long seed = parameterTool.getLong("seed", 42);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    final InputData id = new DataLoader().loadData();

    StoreGenerator sg = new StoreGenerator(id, new SeedFactory(seed));
    final List<Store> stores = new ArrayList<>();
    for (int i = 0; i < nStores; i++) {
      stores.add(sg.generate());
    }
    CustomerGenerator cg = new CustomerGenerator(id, stores, new SeedFactory(seed));
    final List<Customer> customers = new ArrayList<>();
    for (int i = 0; i < nStores; i++) {
      customers.add(cg.generate());
    }

//    env.registerType(com.github.rnowling.bps.datagenerator.datamodels.Product.class);
//    env.registerType(com.github.rnowling.bps.datagenerator.datamodels.Transaction.class);
//    env.registerType(com.github.rnowling.bps.datagenerator.datamodels.Pair.class);
    
    //now need to put customers into n partitions, and have each partition run a generator.
    DataStream<Customer> data = env.fromCollection(customers);

    System.out.println("-- xyz now mapping...." + data);

    SingleOutputStreamOperator<List<Transaction>, ?> so = data.map(
        new MapFunction<Customer, List<Transaction>>() {
          public List<Transaction> map(Customer value) throws Exception {
            Collection<ProductCategory> products = id.getProductCategories();
            PurchasingProfileGenerator profileGen =
                new PurchasingProfileGenerator(products, new SeedFactory(1));

            PurchasingProfile profile = profileGen.generate();
            TransactionGenerator transGen =
                new TransactionGenerator(value, profile, stores, products, new SeedFactory(1));
            List<Transaction> transactions = new ArrayList<>();
            Transaction transaction = transGen.generate();
            transactions.add(transaction);

            //Create a list of this customer's transactions for the time period
            while (transaction.getDateTime() < simulationLength) {
              if (transaction.getDateTime() > BURNIN_TIME) {
                transactions.add(transaction);
              }
              transaction = transGen.generate();
            }
            return transactions;
          }
        });

    so.write(new TransactionListOutputFormat(inputFile), 0L);
    env.execute();
  }

  // Sink reusable for batch programs
  public static class TransactionListOutputFormat extends FileOutputFormat<List<Transaction>> {

    private transient Writer wrt;

    public TransactionListOutputFormat(String outputPath) {
      super(new Path(outputPath));
      this.setWriteMode(FileSystem.WriteMode.OVERWRITE);
    }

    @Override
    public void open(int i, int i1) throws IOException {
      super.open(i, i1);
      wrt = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096));
    }

    @Override
    public void writeRecord(List<Transaction> transactions) throws IOException {
      for (Transaction transaction : transactions) {
        FlinkSerTransaction fst = new FlinkSerTransaction(transaction);
        //TODO add native serialization support to bps so that this hack is unnecessary.
        wrt.write(MAPPER.writeValueAsString(fst));
        wrt.write("\n");
      }
    }

    @Override
    public void close() throws IOException {
      if (wrt != null) {
        wrt.flush();
        wrt.close();
      }
      super.close();
    }
  }
}
