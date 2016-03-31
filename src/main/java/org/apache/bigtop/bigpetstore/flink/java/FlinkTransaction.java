package org.apache.bigtop.bigpetstore.flink.java;

import com.github.rnowling.bps.datagenerator.datamodels.*;
import com.github.rnowling.bps.datagenerator.datamodels.inputs.ZipcodeRecord;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.util.*;

public class FlinkTransaction {

	public Double dateTime;
	public Customer customer;
	public Integer id;
	public Store store;
	public List<String> products = new ArrayList<>();

	public FlinkTransaction() {} // empty constructor to satisfy Flink POJO requirements

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
			products.add(theProduct.toString());
		}
	}

	public FlinkTransaction(String JSONString){
		try {
			JSONObject transactionJSON = new JSONObject(JSONString);

			//dateTime
			dateTime = transactionJSON.getDouble("dateTime");

			//customer
			JSONObject customerJSON = transactionJSON.getJSONObject("customer");
			int customerId = customerJSON.getInt("id");
			Pair<String, String> customerName = new Pair<>(customerJSON.getJSONArray("name").get(0).toString(),
					customerJSON.getJSONArray("name").get(1).toString());
			JSONObject customerLocJSON = customerJSON.getJSONObject("location");
			String customerZipCode = customerLocJSON.getString("zipcode");
			Pair<Double, Double> customerCoordinates = new Pair<>((Double) customerLocJSON.getJSONArray("coordinates").get(0),
					(Double) customerLocJSON.getJSONArray("coordinates").get(1));
			String customerCity = customerLocJSON.getString("city");
			String customerState = customerLocJSON.getString("state");
			double customerMHI = customerLocJSON.getDouble("medianHouseholdIncome");
			long customerPop = customerLocJSON.getLong("population");
			ZipcodeRecord customerLoc = new ZipcodeRecord(customerZipCode, customerCoordinates, customerCity,
					customerState, customerMHI, customerPop);
			customer = new Customer(customerId, customerName, customerLoc);

			//id
			id = transactionJSON.getInt("id");

			//store
			JSONObject storeJSON = transactionJSON.getJSONObject("customer");
			int storeId = storeJSON.getInt("id");
			String storeName = storeJSON.getString("name");
			JSONObject storeLocJSON = customerJSON.getJSONObject("location");
			String storeZipCode = storeLocJSON.getString("zipcode");
			Pair<Double, Double> storeCoordinates = new Pair<>((Double) storeLocJSON.getJSONArray("coordinates").get(0),
					(Double) storeLocJSON.getJSONArray("coordinates").get(1));
			String storeCity = storeLocJSON.getString("city");
			String storeState = storeLocJSON.getString("state");
			double storeMHI = storeLocJSON.getDouble("medianHouseholdIncome");
			long storePop = storeLocJSON.getLong("population");
			ZipcodeRecord storeLoc = new ZipcodeRecord(storeZipCode, storeCoordinates, storeCity,
					storeState, storeMHI, storePop);
			store = new Store(storeId, storeName, storeLoc);

			//products
			JSONArray productsJSON = transactionJSON.getJSONArray("products");
			products = new ArrayList<>();
			for (int i = 0; i < productsJSON.length(); i++){
				products.add(productsJSON.getString(0));
			}

		} catch (JSONException e){
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		JSONObject transactionJSON = new JSONObject();

		try {
			//dateTime
			transactionJSON.put("dateTime", dateTime);

			//customer
			JSONObject customerJSON = new JSONObject();
			customerJSON.put("id", getCustomer().getId());
			customerJSON.put("name", Arrays.asList(getCustomer().getName().getFirst(),
					getCustomer().getName().getSecond()));
			JSONObject customerLocJSON = new JSONObject();
			ZipcodeRecord customerLoc = getCustomer().getLocation();
			customerLocJSON.put("zipcode", customerLoc.getZipcode());
			customerLocJSON.put("coordinates", Arrays.asList(customerLoc.getCoordinates().getFirst(),
					customerLoc.getCoordinates().getSecond()));
			customerLocJSON.put("city", customerLoc.getCity());
			customerLocJSON.put("state", customerLoc.getState());
			customerLocJSON.put("medianHouseholdIncome", customerLoc.getMedianHouseholdIncome());
			customerLocJSON.put("population", customerLoc.getPopulation());
			customerJSON.put("location", customerLocJSON);
			transactionJSON.put("customer", customerJSON);

			//id
			transactionJSON.put("id", getId());

			//store
			JSONObject storeJSON = new JSONObject();
			storeJSON.put("id", getStore().getId());
			storeJSON.put("name", getStore().getName());
			JSONObject storeLocJSON = new JSONObject();
			ZipcodeRecord storeLoc = getStore().getLocation();
			storeLocJSON.put("zipcode", storeLoc.getZipcode());
			storeLocJSON.put("coordinates", Arrays.asList(storeLoc.getCoordinates().getFirst(),
					storeLoc.getCoordinates().getSecond()));
			storeLocJSON.put("city", storeLoc.getCity());
			storeLocJSON.put("state", storeLoc.getState());
			storeLocJSON.put("medianHouseholdIncome", storeLoc.getMedianHouseholdIncome());
			storeLocJSON.put("population", storeLoc.getPopulation());
			storeJSON.put("location", storeLocJSON);
			transactionJSON.put("store", storeJSON);

			//products
			transactionJSON.put("products", getProducts());
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
		return transactionJSON.toString();
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

	public List<String> getProducts() {
		return products;
	}

	public void setProducts(List<String> products) {
		this.products = products;
	}

	public Double getDateTime() {
		return dateTime;
	}

	public void setDateTime(Double dateTime) {
		this.dateTime = dateTime;
	}

}

