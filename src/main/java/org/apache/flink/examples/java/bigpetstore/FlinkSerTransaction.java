package org.apache.flink.examples.java.bigpetstore;

import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.Product;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;

import java.util.*;

/**
 * Created by jayvyas on 10/4/15.
 */
public class FlinkSerTransaction {

  Double dateTime;
  Customer customer;
  Integer id;
  Store store;
  List<Map> products = new ArrayList<>();

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

  public List<Map> getProducts() {
    return products;
  }

  public void setProducts(List<Map> products) {
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
