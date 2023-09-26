package com.innovativeintelli.eod.portfolio.calculator.util;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class PortfolioCSVGenerator {

    private static final String[] STOCK_SYMBOLS = {
    "AAPL",  // Apple Inc.
    "MSFT",  // Microsoft Corporation
    "GOOGL", // Alphabet Inc. (Class A)
    "AMZN",  // Amazon.com Inc.
    "TSLA",  // Tesla, Inc.
    "FB",    // Meta Platforms, Inc. (formerly Facebook, Inc.)
    "NVDA",  // NVIDIA Corporation
    "INTC",  // Intel Corporation
    "ADBE",  // Adobe Inc.
    "PYPL",  // PayPal Holdings, Inc.
    "NFLX",  // Netflix, Inc.
    "CMCSA", // Comcast Corporation
    "CSCO",  // Cisco Systems, Inc.
    "PEP",   // PepsiCo, Inc.
    "TSM",   // Taiwan Semiconductor Manufacturing Company Ltd.
    "AVGO",  // Broadcom Inc.
    "QCOM",  // Qualcomm Incorporated
    "COST",  // Costco Wholesale Corporation
    "TXN",   // Texas Instruments Incorporated
    "AMGN",  // Amgen Inc.
    "JD",    // JD.com Inc.
    "INTU",  // Intuit Inc.
    "BKNG",  // Booking Holdings Inc.
    "SIRI",  // Sirius XM Holdings Inc.
    "FOX",   // Fox Corporation (Class B)
    "EBAY",  // eBay Inc.
    "NTES",  // NetEase, Inc.
    "ATVI",  // Activision Blizzard, Inc.
    "MU",    // Micron Technology, Inc.
    "NXPI",  // NXP Semiconductors N.V.
    "MDLZ",  // Mondelez International, Inc.
    // You can continue adding more NASDAQ stock symbols as needed
};

    private static final int NUM_ROWS = 100; // Specify the number of rows you want in the CSV file

    public static void main(String[] args) {
        try {
            FileWriter writer = new FileWriter("portfolio.csv");
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader("portfolioid", "cashValue", "symbol", "holdingValue"));

            Random random = new Random();

            for (int i = 1; i <= NUM_ROWS; i++) {
                int portfolioId = i;
                double cashValue = 1000 + random.nextDouble() * 9000; // Random cash value between 1000 and 10000
                String stockSymbol = STOCK_SYMBOLS[random.nextInt(STOCK_SYMBOLS.length)];
                int numberOfShares = random.nextInt(100); // Random number of shares between 0 and 99

                csvPrinter.printRecord(portfolioId, cashValue, stockSymbol, numberOfShares);
            }

            csvPrinter.flush();
            csvPrinter.close();

            System.out.println("CSV file 'portfolio.csv' generated successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
