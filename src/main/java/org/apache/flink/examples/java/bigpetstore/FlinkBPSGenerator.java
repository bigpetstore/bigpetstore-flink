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

import java.util.Collection;
import java.util.List;

import com.github.rnowling.bps.datagenerator.*;
import com.github.rnowling.bps.datagenerator.datamodels.PurchasingProfile;
import com.github.rnowling.bps.datagenerator.datamodels.PurchasingProfileBuilder;
import com.github.rnowling.bps.datagenerator.datamodels.*;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.InputData;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ProductCategory;
import com.github.rnowling.bps.datagenerator.framework.SeedFactory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 */
@SuppressWarnings("serial")
public class FlinkBPSGenerator {

    public DataSet<String> generateData(ExecutionEnvironment env) {

        return null;
    }


    public void writeData(DataSet env) {


    }

    public static void main(String[] args) throws Exception {

        final int simulationLength = 10;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int nStores = 100;

        FlinkBPSGenerator generator = new FlinkBPSGenerator();
        final InputData id = new DataLoader().loadData();
        final SeedFactory seedFactory = new SeedFactory(1);
        StoreGenerator sg = new StoreGenerator(id, seedFactory);
        final List<Store> stores = Lists.newArrayList();
        for (int i = 0; i < nStores; i++) {
            stores.add(sg.generate());
        }
        CustomerGenerator cg = new CustomerGenerator(id, stores, seedFactory);
        final List<Customer> customers = Lists.newArrayList();
        for (int i = 0; i < nStores; i++) {
            customers.add(cg.generate());
        }

        //now need to put customers into n partitions, and have each partition run a generator.
        DataStream<Customer> data = env.fromCollection(customers);
        data.map(
                new MapFunction<Customer, List<Transaction>>() {
                    public List<Transaction> map(Customer value) throws Exception {

                        Collection<ProductCategory> products = id.getProductCategories();
                        PurchasingProfileGenerator profileGen = new PurchasingProfileGenerator(products, seedFactory);
                        PurchasingProfile profile = profileGen.generate();
                        TransactionGenerator transGen = new TransactionGenerator(value, profile, stores, products, seedFactory);
                        List<Transaction> transactions = Lists.newArrayList();
                        Transaction transaction = transGen.generate();
                        transactions.add(transaction);
                        //Create a list of this customer's transactions for the time period
                        while (transaction.getDateTime() < simulationLength) {
                            //TODO implement burn in time like we do in bps-spark
                            transactions.add(transaction);
                            transaction = transGen.generate();
                        }
                        return transactions;
                    }
                });


    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String textPath;
    private static String outputPath;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            // parse input arguments
            fileOutput = true;
            if (args.length == 2) {
                textPath = args[0];
                outputPath = args[1];
            } else {
                System.err.println("Usage: WordCount <text path> <result path>");
                return false;
            }
        } else {
            System.out.println("Executing WordCount example with built-in default data.");
            System.out.println("  Provide parameters to read input data from a file.");
            System.out.println("  Usage: WordCount <text path> <result path>");
        }
        return true;
    }

    private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readTextFile(textPath); // read the text file from given input path
        }
        return null;
    }
}
