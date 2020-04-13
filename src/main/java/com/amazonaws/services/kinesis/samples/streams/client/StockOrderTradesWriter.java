/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samples.streams.client;


import java.util.concurrent.ExecutionException;



import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.samples.streams.model.StockOrder;
import com.amazonaws.services.kinesis.samples.streams.model.StockTrade;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.KinesisClientUtil;

/**
 * Continuously sends simulated stock trades to Kinesis
 *
 */
public class StockOrderTradesWriter {

    private static final Log LOG = LogFactory.getLog(StockOrderTradesWriter.class);

    private static void checkUsage(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: " + StockOrderTradesWriter.class.getSimpleName()
                    + " <stream name> <stream name> <region>");
            System.exit(1);
        }
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName Name of stream
     */
    private static void validateStream(KinesisAsyncClient kinesisClient, String streamName) {
        try {
            DescribeStreamRequest describeStreamRequest =  DescribeStreamRequest.builder().streamName(streamName).build();
            DescribeStreamResponse describeStreamResponse = kinesisClient.describeStream(describeStreamRequest).get();
            if(!describeStreamResponse.streamDescription().streamStatus().toString().equals("ACTIVE")) {
                System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
                System.exit(1);
            }
        }catch (Exception e) {
            System.err.println("Error found while describing the stream " + streamName);
            System.err.println(e);
            System.exit(1);
        }
    }

    /**
     * Uses the Kinesis client to send the stock trade to the given stream.
     *
     * @param trade instance representing the stock trade
     * @param kinesisClient Amazon Kinesis client
     * @param streamName Name of stream
     */
   

    
    		private static void sendStockTrade(StockOrder order, KinesisAsyncClient kinesisClient,
    	            String orderStream, String tradeStream) {
    			
    			StockTrade trade = order.getStockTrade();
    	        
    	        order.setStockTrade(null);
    			
    	        byte[] bytes = order.toJsonAsBytes();
    	        
    	        // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
    	        if (bytes == null) {
    	            LOG.warn("Could not get JSON bytes for stock trade");
    	            return;
    	        }
    	        
    	        

    	        LOG.info("Putting order: " + order.toString());
    	        PutRecordRequest request = PutRecordRequest.builder()
    	                .partitionKey(order.getTickerSymbol()) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
    	                .streamName(orderStream)
    	                .data(SdkBytes.fromByteArray(bytes))
    	                .build();
    	        try {
    	            kinesisClient.putRecord(request).get();
    	        } catch (InterruptedException e) {
    	            LOG.info("Interrupted, assuming shutdown.");
    	        } catch (ExecutionException e) {
    	            LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
    	        }
    	        
    	        
    	        
    	       // LOG.info(trade.);
    	        byte[] tradeBytes = trade.toJsonAsBytes();
    	        
    	        LOG.info("Putting trade: " + trade.toString());
    	         request = PutRecordRequest.builder()
    	                .partitionKey(trade.getTickerSymbol()) // We use the ticker symbol as the partition key, explained in the Supplemental Information section below.
    	                .streamName(tradeStream)
    	                .data(SdkBytes.fromByteArray(tradeBytes))
    	                .build();
    	        try {
    	            kinesisClient.putRecord(request).get();
    	        } catch (InterruptedException e) {
    	            LOG.info("Interrupted, assuming shutdown.");
    	        } catch (ExecutionException e) {
    	            LOG.error("Exception while sending data to Kinesis. Will try again next cycle.", e);
    	        }
    	    }
    	                    
    		
    		
                                    	   
    
  /*
   * to run from command line 
   * 1. export this class as runnable jar 
   * 2. java -cp ./StockTradesWriter.jar com.amazonaws.services.kinesis.samples.stocktrades.writer.StockTradesWriter StockTradeStream us-west-2
   * 
   */
    		
    public static void main(String[] args) throws Exception {
        checkUsage(args);

        String orderStream = args[0];
        String tradeStream = args[1];
        String regionName = args[2];
        Region region = Region.of(regionName);
        if (region == null) {
            System.err.println(regionName + " is not a valid AWS region.");
            System.exit(1);
        }

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));

        // Validate that the stream exists and is active
        validateStream(kinesisClient, orderStream);
        
        validateStream(kinesisClient, tradeStream);

        // Repeatedly send stock order and trade with a 100 milliseconds wait in between
        StockOrderTradeGenerator stockOrderTradeGenerator = new StockOrderTradeGenerator();
        while(true) {
            StockOrder order = stockOrderTradeGenerator.getRandomStock();
            sendStockTrade(order, kinesisClient, orderStream, tradeStream);
            Thread.sleep(100);
        }
    }

}
