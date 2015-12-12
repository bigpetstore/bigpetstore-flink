package org.apache.flink.examples.java.bigpetstore;

import com.github.rnowling.bps.datagenerator.datamodels.Customer;
import com.github.rnowling.bps.datagenerator.datamodels.Product;
import com.github.rnowling.bps.datagenerator.datamodels.Store;
import com.github.rnowling.bps.datagenerator.datamodels.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FlinkTransaction {

	public Double dateTime;
	public Customer customer;
	public Integer id;
	public Store store;
	public List<Map<String,Object>> products = new ArrayList<>();

	public FlinkTransaction(Transaction t) {
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

	@Override
	public String toString() {
		return  "{id=" + id +
				", dateTime=" + dateTime +
				", customer={id=" + customer.getId() +
				", location=[" + customer.getLocation().getCoordinates().getFirst() + ", " + customer.getLocation().getCoordinates().getSecond() + "]" +
				", name=[" + customer.getName().getFirst() + ", " + customer.getName().getSecond() + "]}" +
				", store={id=" + store.getId() +
				", location=[" + store.getLocation().getCoordinates().getFirst() + ", " + store.getLocation().getCoordinates().getSecond() + "]" +
				", name=" + store.getName() + "}" +
				", products=" + products +
				'}';
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

	public FlinkTransaction() {
	}

}

