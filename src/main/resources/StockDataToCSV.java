package com.innovativeintelli.eod.portfolio.calculator.util;

import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONTokener;

import scala.util.parsing.json.JSONObject;

@SuppressWarnings("deprecation")
public class StockDataToCSV {

    private static final String ALPHA_VANTAGE_API_KEY = "YOUR_ALPHA_VANTAGE_API_KEY";
    
    public static void main(String[] args) {
        List<Stock> stocks = new ArrayList<>();
        
        // Replace these symbols with the actual symbols you want to fetch
        String[] symbols = {"AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"};
        
        for (String symbol : symbols) {
            Stock stock = fetchStockData(symbol);
            if (stock != null) {
                stocks.add(stock);
            }
        }
        
        writeStocksToCSV(stocks);
    }

    private static Stock fetchStockData(String symbol) {
        try {
            String apiUrl = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + symbol +
                            "&interval=1min&apikey=" + ALPHA_VANTAGE_API_KEY;
            
            URL url = new URL(apiUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            int responseCode = conn.getResponseCode();
            
            if (responseCode == 200) {
                Scanner scanner = new Scanner(url.openStream());
                String response = scanner.useDelimiter("\\A").next();
                scanner.close();
                
                JSONObject json = new JSONObject(new JSONTokener(response));
                JSONObject timeSeries = json.getJSONObject("Time Series (1min)");
                String lastClosedPrice = timeSeries.getJSONObject(timeSeries.keys().next()).getString("4. close");
                
                return new Stock(symbol, lastClosedPrice);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return null;
    }

    private static void writeStocksToCSV(List<Stock> stocks) {
        try (FileWriter writer = new FileWriter("stock_data.csv");
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                     .withHeader("stockId", "symbol", "lastclosedprice"))) {
            
            for (Stock stock : stocks) {
                csvPrinter.printRecord(stock.getSymbol(), stock.getSymbol(), stock.getLastClosedPrice());
            }
            
            csvPrinter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class Stock {
        private String symbol;
        private String lastClosedPrice;
        
        public Stock(String symbol, String lastClosedPrice) {
            this.symbol = symbol;
            this.lastClosedPrice = lastClosedPrice;
        }
        
        public String getSymbol() {
            return symbol;
        }
        
        public String getLastClosedPrice() {
            return lastClosedPrice;
        }
    }
}
