package consumer;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import model.StockTrade;
import model.StockTrade.TradeType;

public class StockStats {

	// Keeps count of trades for each ticker symbol for each trade type
	private EnumMap<TradeType, Map<String, Long>> countsByTradeType;

	// Keeps the ticker symbol for the most popular stock for each trade type
	private EnumMap<TradeType, String> mostPopularByTradeType;

	// Ticker symbol of the stock that had the largest quantity of shares sold
	private String largestSellOrderStock;

	// Quantity of shares for the largest sell order trade
	private long largestSellOrderQuantity;

	public StockStats() {
		countsByTradeType = new EnumMap<TradeType, Map<String, Long>>(TradeType.class);
		for (TradeType tradeType : TradeType.values()) {
			countsByTradeType.put(tradeType, new HashMap<String, Long>());
		}
		mostPopularByTradeType = new EnumMap<TradeType, String>(TradeType.class);
	}

	public void addStockTrade(StockTrade trade) {
		// update buy/sell count
		TradeType type = trade.getTradeType();
		Map<String, Long> counts = countsByTradeType.get(type);
		Long count = counts.get(trade.getTickerSymbol());
		if (count == null) {
			count = 0L;
		}
		counts.put(trade.getTickerSymbol(), ++count);

		// update most popular stock
		String mostPopular = mostPopularByTradeType.get(type);
		if (mostPopular == null || countsByTradeType.get(type).get(mostPopular) < count) {
			mostPopularByTradeType.put(type, trade.getTickerSymbol());
		}

		// update largest sell order
		if (type == TradeType.SELL) {
			if (largestSellOrderStock == null || trade.getQuantity() > largestSellOrderQuantity) {
				largestSellOrderStock = trade.getTickerSymbol();
				largestSellOrderQuantity = trade.getQuantity();
			}
		}
	}

	public String toString() {
		return String.format(
				"Most popular stock being bought: %s, %d buys.%n" + "Most popular stock being sold: %s, %d sells.%n"
						+ "Largest sell order: %d shares of %s.",
				getMostPopularStock(TradeType.BUY), getMostPopularStockCount(TradeType.BUY),
				getMostPopularStock(TradeType.SELL), getMostPopularStockCount(TradeType.SELL), largestSellOrderQuantity,
				largestSellOrderStock);
	}

	private String getMostPopularStock(TradeType tradeType) {
		return mostPopularByTradeType.get(tradeType);
	}

	private Long getMostPopularStockCount(TradeType tradeType) {
		String mostPopular = getMostPopularStock(tradeType);
		return countsByTradeType.get(tradeType).get(mostPopular);
	}

}
