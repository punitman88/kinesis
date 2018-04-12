package producer;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

import configuration.ConfigurationUtils;
import configuration.CredentialUtils;
import model.StockTrade;

public class StockTradesWriter {

	private static final Log LOG = LogFactory.getLog(StockTradesWriter.class);

	private static void checkUsage(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: " + StockTradesWriter.class.getSimpleName() + " <stream name> <region>");
			System.exit(1);
		}
	}

	private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
			if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
				System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
				System.exit(1);
			}
		} catch (ResourceNotFoundException e) {
			System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
			System.err.println(e);
			System.exit(1);
		} catch (Exception e) {
			System.err.println("Error found while describing the stream " + streamName);
			System.err.println(e);
			System.exit(1);
		}
	}

	private static void sendStockTrade(StockTrade trade, AmazonKinesis kinesisClient, String streamName) {
		byte[] bytes = trade.toJsonAsBytes();
		if (bytes == null) {
			LOG.warn("Could not get JSON bytes for stock trade");
			return;
		}
		LOG.info("Putting trade: " + trade.toString());
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		putRecord.setPartitionKey(trade.getTickerSymbol());
		putRecord.setData(ByteBuffer.wrap(bytes));
		try {
			kinesisClient.putRecord(putRecord);
		} catch (AmazonClientException ex) {
			LOG.warn("Error sending record to Amazon Kinesis.", ex);
		}
	}

	// Run by passing two arguments <stream name> <region> e.g. StockTradeStream us-west-2
	public static void main(String[] args) throws Exception {

		checkUsage(args);
		String streamName = args[0];
		String regionName = args[1];
		Region region = RegionUtils.getRegion(regionName);
		if (region == null) {
			System.err.println(regionName + " is not a valid AWS region.");
			System.exit(1);
		}

		AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
		clientBuilder.setRegion(regionName);
		clientBuilder.setCredentials(CredentialUtils.getCredentialsProvider("kinesis-producer"));
		clientBuilder.setClientConfiguration(ConfigurationUtils.getClientConfigWithUserAgent());
		AmazonKinesis kinesisClient = clientBuilder.build();

		// Validate that the stream exists and is active
		validateStream(kinesisClient, streamName);

		// Repeatedly send stock trades with a 100 milliseconds wait in between
		StockTradeGenerator stockTradeGenerator = new StockTradeGenerator();
		while (true) {
			StockTrade trade = stockTradeGenerator.getRandomTrade();
			sendStockTrade(trade, kinesisClient, streamName);
			Thread.sleep(100);
		}

	}

}
