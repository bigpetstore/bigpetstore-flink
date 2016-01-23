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

import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.Product;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;

import java.util.*;

public class FlinkSerTransaction {

  Double dateTime;
  Customer customer;
  Integer id;
  Store store;
  List<Map<String,Object>> products = new ArrayList<>();

  public FlinkSerTransaction(Transaction t) {
    dateTime = t.getDateTime();
    customer = t.getCustomer();
    id = t.getId();
    store = t.getStore();

    //Store each product as a map, i.e.
    // {name="kitty-poo-bags", brand="alpo", size="M", cost="100$"}...
    for (Product p : t.getProducts()) {
      Map<String, Object> theProduct = new TreeMap<>();
      for (String s : p.getFieldNames()) {
        theProduct.put(s, p.getFieldValue(s));
      }
      products.add(theProduct);
    }

  }

  public Customer getCustomer() {
    return customer;
  }

  public void setCustomer(Customer customer) {
    this.customer = customer;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Store getStore() {
    return store;
  }

  public void setStore(Store store) {
    this.store = store;
  }

  public List<Map<String, Object>> getProducts() {
    return products;
  }

  public void setProducts(List<Map<String, Object>> products) {
    this.products = products;
  }

  public Double getDateTime() {
    return dateTime;
  }

  public void setDateTime(Double dateTime) {
    this.dateTime = dateTime;
  }

  public FlinkSerTransaction() {

  }


}
