package consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class StockTradeRecordProcessorFactory implements IRecordProcessorFactory {

	public StockTradeRecordProcessorFactory() {
		super();
	}

	@Override
	public IRecordProcessor createProcessor() {
		return new StockTradeRecordProcessor();
	}

}
