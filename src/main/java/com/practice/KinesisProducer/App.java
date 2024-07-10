package com.practice.KinesisProducer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.amazonaws.services.kinesis.AmazonKinesis;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.practice.KinesisProducer.aws.AwsKinesisClient;
import com.practice.KinesisProducer.model.Order;

/**
 *ajay wadhara channel
 *
 */
public class App 
{
	
	List<String> productList = new ArrayList<String>();
	
	Random random = new Random();
	
    public static void main( String[] args ) throws InterruptedException
    {
        App app = new App();
        app.populateProductList();   
        
        //1. get client
        AmazonKinesis kinesisClient = AwsKinesisClient.getKinesisClient(); 
        while(true) {
        app.sendData(kinesisClient);
        Thread.sleep(5000);
        }
    }
    
    private void sendData(AmazonKinesis kinesisClient) {
    	
        //2. PutRecordsRequest
       
        PutRecordsRequest recordsRequest = new PutRecordsRequest();
        recordsRequest.setStreamName("order-stream");
        recordsRequest.setRecords(getRecordsRequestList());
        
        //3. putRecord or putRecords - 500 records with single api call
        PutRecordsResult results = kinesisClient.putRecords(recordsRequest);
        if(results.getFailedRecordCount() > 1) {
        	System.out.println("Error occured for records "+results.getFailedRecordCount());
        }else {
        	System.out.println("Data sent successfully...");
        }
        //System.out.println(results);
        
        //retry mechanism
        int failedRecords = results.getFailedRecordCount();
        for(PutRecordsResultEntry result: results.getRecords()) {
        	if(result.getErrorCode() != null) {
        		//there is some problem
        	}
        }
    }
    
    private List<PutRecordsRequestEntry> getRecordsRequestList(){
    	//create Gson object to convert pojo data to json 
    	Gson gson = new GsonBuilder().setPrettyPrinting().create();
    	
    	List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
    	
    	for(Order order: getOrderList()) {
    		PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
    		requestEntry.setData(ByteBuffer.wrap(gson.toJson(order).getBytes()));
    		requestEntry.setPartitionKey(UUID.randomUUID().toString());  
    		putRecordsRequestEntries.add(requestEntry);
    	}
		return putRecordsRequestEntries;
    }
    
    private List<Order> getOrderList(){
    	List<Order> orders = new ArrayList<>();
    	for(int i=0;i<500;i++) {
    		Order order = new Order();
    		order.setOrderId(random.nextInt());
    		order.setProduct(productList.get(random.nextInt(productList.size())));
    		order.setQuantity(random.nextInt(20));
    		orders.add(order);
    	}
    	return orders;
    }
    
    private void populateProductList() {
    	productList.add("shirt");
    	productList.add("t-shirt");
    	productList.add("shorts");
    	productList.add("tie");
    	productList.add("shoes");
    	productList.add("jeans");
    	productList.add("belt");
    }
}
