package com.innovativeintelli.eod.portfolio.calculator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

import com.innovativeintelli.eod.portfolio.calculator.model.PortfolioHolding;
import com.innovativeintelli.eod.portfolio.calculator.model.PortfolioValue;
import com.innovativeintelli.eod.portfolio.calculator.model.StockPrice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;



public class EODPortfolioCalculator {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\aaati\\Documents\\Hadoop");

        SparkSession spark = SparkSession.builder()
            .appName("EOD Portfolio Calculator")
            .config("spark.some.config.option", "some-value")
            .master("local[*]")
            .getOrCreate();

        runEODPortfolioCalculator(spark);

        spark.stop();
    }

    private static void runEODPortfolioCalculator(SparkSession spark) {
        List<PortfolioValue> portfolioValues = new ArrayList<>();

		// TODO Auto-generated method stub
		List<StockPrice> eodStockPrices = Arrays.asList(new StockPrice(1, "AAPL", 125.68), new StockPrice(2, "NVDA", 400.76),
				new StockPrice(3, "MSFT", 200.68), new StockPrice(4, "GOOG", 312.68), new StockPrice(5, "TSLA", 60.98),
				new StockPrice(6, "SPY", 30.89), new StockPrice(7, "QQQ", 80.68), new StockPrice(8, "VOO", 50.12),
				new StockPrice(9, "VGT", 100.34), new StockPrice(10, "SOFI", 125.87));
		
		Map<String, Double> holding1 = new HashMap<String, Double>();
		holding1.put("AAPL", 150.00);
		holding1.put("NVDA", 100.00);
		holding1.put("TSLA", 160.00);
		holding1.put("MSFT", 200.00);
		holding1.put("GOOG", 150.50);
		holding1.put("SPY", 300.00);
		holding1.put("QQQ", 800.00);
		holding1.put("VOO", 600.00);
		holding1.put("VGT", 200.00);
		holding1.put("SOFI", 100.00);
	
		
		List<PortfolioHolding> portfolioHoldings = Arrays.asList(
				new PortfolioHolding(1L, holding1, 1000.90),
				new PortfolioHolding(2L, holding1, 11000.90),
				new PortfolioHolding(3L, holding1, 10300.90),
				new PortfolioHolding(4L, holding1, 103200.90),
				new PortfolioHolding(5L, holding1, 10070.90),
				new PortfolioHolding(6L, holding1, 103400.90),
				new PortfolioHolding(7L, holding1, 100340.90),
				new PortfolioHolding(8L, holding1, 100340.90),
				new PortfolioHolding(9L, holding1, 100340.90),
				new PortfolioHolding(10L, holding1, 1023200.90),
				new PortfolioHolding(11L, holding1, 100230.90),
				new PortfolioHolding(12L, holding1, 100230.90));

        Dataset<Row> portfolios = spark.createDataFrame(portfolioHoldings, PortfolioHolding.class);

        Dataset<Row> stockPrices = spark.createDataFrame(eodStockPrices, StockPrice.class);
  
     // Explode the holding map to create separate rows for each stockSymbol
        Dataset<Row> explodedPortfolios = portfolios
            .selectExpr("cashValue", "portfolioId", "explode(holdings) as (symbol, holdingValue)");

        // Join explodedPortfolios with stockPrices based on stockSymbol
        Dataset<Row> joinedData = explodedPortfolios
            .join(stockPrices, explodedPortfolios.col("symbol").equalTo(stockPrices.col("symbol")));

        // Define a User-Defined Function (UDF) to calculate the value
        spark.udf().register("calculateValue", (Double holdingValue, Double price, Double cashValue) -> cashValue + (holdingValue * price), DataTypes.DoubleType);

        // Calculate portfolio values using the UDF
        Dataset<Row> calculatedData = joinedData.withColumn(
            "value",
            org.apache.spark.sql.functions.callUDF("calculateValue", joinedData.col("holdingValue"), joinedData.col("price"), joinedData.col("cashValue"))
        );

        // Sum portfolio values
        Dataset<Row> summedData = calculatedData.groupBy("portfolioId")
            .agg(org.apache.spark.sql.functions.sum("value").as("endOfDayValue"));

        // Collect and display results
        List<PortfolioValue> calculatedPortfolioValues = summedData
            .as(Encoders.bean(PortfolioValue.class))
            .collectAsList();

        calculatedPortfolioValues.forEach(portfolioValue -> {
            portfolioValues.add(new PortfolioValue(portfolioValue.getPortfolioId(), portfolioValue.getEndOfDayValue()));
            System.out.println("Portfolio Id " + portfolioValue.getPortfolioId() + " Portfolio value " + portfolioValue.getEndOfDayValue());
        });
        
        Scanner scanner = new Scanner(System.in);
        scanner.hasNext();
    }
}
