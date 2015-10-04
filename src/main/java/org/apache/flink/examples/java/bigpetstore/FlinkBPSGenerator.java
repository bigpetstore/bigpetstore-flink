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

package org.apache.flink.examples.java.bigpetstore;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rnowling.bps.datagenerator.CustomerGenerator;
import com.github.rnowling.bps.datagenerator.DataLoader;
import com.github.rnowling.bps.datagenerator.PurchasingProfileGenerator;
import com.github.rnowling.bps.datagenerator.StoreGenerator;
import com.github.rnowling.bps.datagenerator.TransactionGenerator;
import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.Product;
import com.github.rnowling.bps.datagenerator.datamodels.PurchasingProfile;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 */
@SuppressWarnings("serial")
public class FlinkBPSGenerator {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public DataSet<String> generateData(ExecutionEnvironment env) {

    return null;
  }


  public void writeData(DataSet<String> env) {


  }

  public static final class ProductSerializer extends com.esotericsoftware.kryo.Serializer<Product> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, com.github.rnowling.bps.datagenerator.datamodels.Product object) {
      for (String f : object.getFieldNames()) {
        output.writeAscii(f + "," + object.getFieldValue(f));
      }
    }

    @Override
    public Product read(Kryo kryo, Input input, Class<com.github.rnowling.bps.datagenerator.datamodels.Product> type) {
      String[] csv = input.readString().split(",");
      Map<String, Object> entries = new HashMap<>();
      for (int i = 0; i < csv.length; i++) {
        if (csv.length > i + 1) {
          //add the product entries one by one.
          entries.put(csv[i], csv[i + 1]);
        } else {
          throw new RuntimeException("Missing value for product " + csv[i] + " in serialized string " + StringUtils.join(csv));
        }
      }
      return new Product(entries);
    }
  }

  public static final class TransactionSerializer extends com.esotericsoftware.kryo.Serializer<Transaction> implements Serializable {
    @Override
    public void write(Kryo kryo, Output output, com.github.rnowling.bps.datagenerator.datamodels.Transaction object) {
      try {
        output.write(SerializationUtils.serialize(object));
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }

    }

    @Override
    public Transaction read(Kryo kryo, Input input, Class<com.github.rnowling.bps.datagenerator.datamodels.Transaction> type) {
      return (Transaction) SerializationUtils.deserialize(input.getInputStream());
    }
  }

  /**
   * Args:
   *  (1) the output file (i.e. /tmp/a => write data files to /tmp/a/1, /tmp/a/2, and so on).
   *  (2) sim length (i.e. 10 => 10 days).
   *  (3) the stores (i.e.  3 => 3 stores randomly created)
   */
  public static void main(String inOutputfile, String inSimlength, String inStores, String... others) throws Exception {

    final String outputfile = inOutputfile;
    final int simulationLength = Integer.parseInt(inSimlength);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    int nStores = Integer.parseInt(inStores);

    FlinkBPSGenerator generator = new FlinkBPSGenerator();
    final InputData id = new DataLoader().loadData();

    //TODO Should we  reuse the seed factory?
    StoreGenerator sg = new StoreGenerator(id, new SeedFactory(1));//see above todo
    final List<Store> stores = new ArrayList<>();
    for (int i = 0; i < nStores; i++) {
      stores.add(sg.generate());
    }
    CustomerGenerator cg = new CustomerGenerator(id, stores, new SeedFactory(1));//see above todo
    final List<Customer> customers = Lists.newArrayList();
    for (int i = 0; i < nStores; i++) {
      customers.add(cg.generate());
    }

    env.registerType(com.github.rnowling.bps.datagenerator.datamodels.Product.class);
    env.registerType(com.github.rnowling.bps.datagenerator.datamodels.Transaction.class);

    //now need to put customers into n partitions, and have each partition run a generator.
    DataStream<Customer> data = env.fromCollection(customers);

    System.out.println("-- xyz now mapping...." + data);

    SingleOutputStreamOperator<List<Transaction>, ?> so = data.map(
        new MapFunction<Customer, List<Transaction>>() {
          public List<Transaction> map(Customer value) throws Exception {

            Collection<ProductCategory> products = id.getProductCategories();

            //TODO reuse seedfactory variable above.
            PurchasingProfileGenerator profileGen = new PurchasingProfileGenerator(products, new SeedFactory(1));
            PurchasingProfile profile = profileGen.generate();
            TransactionGenerator transGen = new TransactionGenerator(value, profile, stores, products, new SeedFactory(1));
            List<Transaction> transactions = Lists.newArrayList();
            Transaction transaction = transGen.generate();
            transactions.add(transaction);
            //Create a list of this customer's transactions for the time period
            while (transaction.getDateTime() < simulationLength) {
              //TODO implement burn in time like we do in bps-spark
              transactions.add(transaction);
              System.out.println("... " + transaction);
              transaction = transGen.generate();
            }
            return transactions;
          }
        });

    so.write((new TransactionListOutputFormat(outputfile)), 0L);
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
