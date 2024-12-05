import ccxt from 'ccxt';
import TelegramBot from 'node-telegram-bot-api';
import { SocksProxyAgent } from 'socks-proxy-agent';
import _ from 'lodash';
import {
  TELEGRAM_API_KEY,
  TELEGRAM_CHAT_ID,
  MIN_SPREAD,
  MIN_VOLUME,
  BUFFER_EXPIRY_TIME,
  ARBITRAGE_PERCENTAGE_THRESHOLD,
  proxies,
  MAX_PROCESS_COUNT,
  MIN_24_VOLUME,
  EXCHANGE_URLS,
  EXCHANGES,
} from './config.js';

// Initialize variables
const opportunityBuffer = {};
let orderBookCache = {}; // Cache for order books

// Initialize exchanges
const exchanges = Object.fromEntries(
  EXCHANGES.map((exchangeName, index) => [
    exchangeName,
    new ccxt[exchangeName.toLowerCase()]({
      rateLimit: 100,
      agent: new SocksProxyAgent(proxies[index % proxies.length]),
    }),
  ])
);

// flags reflect about whether get or not the pairs of ExchangeA-SPOT:ExchangeA-FUTURE
let exchangeFlag = {
  'Gate': false,
  'Mexc': false,
  'Bybit': false,
  'Bitget': false,
  'Okx': false,
}

process.setMaxListeners(MAX_PROCESS_COUNT);

// Helper to split arrays into chunks
function chunkArray(array, numParts) {
  const chunkSize = Math.ceil(array.length / numParts);
  return Array.from({ length: numParts }, (_, index) =>
    array.slice(index * chunkSize, (index + 1) * chunkSize)
  );
}

function chunkify(array, size) {
  return _.chunk(array, size);
}

async function fetchWithRetry(fn, retries = 3, delay = 500) {
  let attempts = 0;
  while (attempts < retries) {
    try {
      return await fn();
    } catch (err) {
      attempts++;
      if (attempts >= retries) {
        console.error('Failed after retries:', err.message);
        throw err; // Propagate error if retries exhausted
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

// Fetch order books with caching
async function fetchOrderBooksWithCache(exchange, pair, exchangeBuyName, exchangeSellName) {
  const cacheKey = `${exchangeBuyName}:${pair}`;
  if (orderBookCache[cacheKey]) {
    return orderBookCache[cacheKey];
  } else {
    try {
      let orderBook = {};
      const fetchOrderBook = async () => {
        if (
          exchange.symbols.includes(pair) &&
          exchangeBuyName.includes('FUTURE') &&
          exchangeSellName.includes('SPOT')
        ) {
          return await exchange.fetchOrderBook(pair.replace('/USDT', '/USDT:USDT'));
        } else if (exchange.symbols.includes(pair)) {
          return await exchange.fetchOrderBook(pair);
        }
  
        throw new Error(`Symbol ${pair} not supported`);
      };
  
      orderBook = await fetchWithRetry(fetchOrderBook, 3, 500);
      orderBookCache[cacheKey] = orderBook; // Save to cache
      return orderBook;
    } catch (error) {
      console.log('Fetch order books with cache error' + error.message);
    }
  }
}

// Fetch order books with cached memory data
async function fetchOrderBooks(exchange, pairs, exchangeBuyName, exchangeSellName, proxy) {
  const orderBooks = {};
  exchange.socksProxy = proxy;

  await Promise.all(
    pairs.map(async (pair) => {
      try {
        orderBooks[pair] = await fetchOrderBooksWithCache(exchange, pair, exchangeBuyName, exchangeSellName);
      } catch (err) {
        console.error(`Error fetching order book for ${pair}:`, err.message);
      }
    })
  );
  return orderBooks;
}

function shouldNotifyOpportunity(pair, newPercentage) {
  const now = Date.now();
  if (!opportunityBuffer[pair]) return true;

  const { lastPercentage, lastTimestamp } = opportunityBuffer[pair];
  const timeElapsed = now - lastTimestamp;
  const percentageChange = Math.abs(newPercentage - lastPercentage);

  return timeElapsed > BUFFER_EXPIRY_TIME || percentageChange >= ARBITRAGE_PERCENTAGE_THRESHOLD;
}

function updateOpportunityBuffer(pair, percentage) {
  opportunityBuffer[pair] = {
    lastPercentage: percentage,
    lastTimestamp: Date.now(),
  };
}

// Compare with Minimum 24h volume
async function filterPerDayVolume(exchange, pairs) {
  try {
    const markets = await exchange.loadMarkets();
    const validPairs = pairs.filter((pair) => markets[pair]);

    if (validPairs.length === 0) return [];

    let tickers;
    try {
      tickers = await exchange.fetchTickers(validPairs);
      await new Promise((resolve) => setTimeout(resolve, 10));
    } catch (err) {
      console.error(`Error fetching tickers for ${exchange.id}:`, err.message);
      return [];
    }

    return validPairs.filter((pair) => {
      const ticker = tickers[pair];
      return ticker && ticker.baseVolume && ticker.baseVolume > MIN_24_VOLUME;
    });
  } catch (err) {
    console.error(`Error in filterPerDayVolume for ${exchange.id}:`, err.message);
    return [];
  }
}

// Calculate profits
function calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName) {
  const profits = [];
  for (const pair in orderBooksA) {
    if (!orderBooksB[pair]) continue;

    const asksA = orderBooksA[pair]?.asks[0];
    const bidsB = orderBooksB[pair]?.bids[0];
    const bidsA = orderBooksA[pair]?.bids[0];
    const asksB = orderBooksB[pair]?.asks[0];

    if (!asksA || !bidsB || !bidsA || !asksB) continue;

    const spreadAB = ((bidsB[0] - asksA[0]) / asksA[0]) * 100;
    const spreadBA = ((bidsA[0] - asksB[0]) / asksB[0]) * 100;

    const maxVolumeAB = Math.min(asksA[1], bidsB[1]);
    const maxVolumeBA = Math.min(asksB[1], bidsA[1]);

    const profitAB = maxVolumeAB * spreadAB / 100;
    const profitBA = maxVolumeBA * spreadBA / 100;

    if (spreadAB >= MIN_SPREAD && maxVolumeAB >= MIN_VOLUME) {
      profits.push({
        pair,
        direction: `${exchangeAName}->${exchangeBName}`,
        exchangeA: exchangeAName,
        exchangeB: exchangeBName,
        spread: spreadAB,
        buyPrice: asksA[0],
        sellPrice: bidsB[0],
        maxVolume: maxVolumeAB,
        profit: profitAB,
      });
    }

    if (spreadBA >= MIN_SPREAD && maxVolumeBA >= MIN_VOLUME) {
      profits.push({
        pair,
        direction: `${exchangeBName}->${exchangeAName}`,
        exchangeA: exchangeBName,
        exchangeB: exchangeAName,
        spread: spreadBA,
        buyPrice: asksB[0],
        sellPrice: bidsA[0],
        maxVolume: maxVolumeBA,
        profit: profitBA,
      });
    }
  }
  return profits;
}

// Calculation between Kucoin and other exchagnes
async function executeArbitrageKucoinCheck(exA) {
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });
  const exchangeA = exchanges[exA];
  const exchangeKucoinSpot = new ccxt.kucoin({
    rateLimit: 100,
    agent: new SocksProxyAgent(proxies[8]),
  });
  const exchangeKucoinFuture = new ccxt.kucoinfutures({
    rateLimit: 100,
    agent: new SocksProxyAgent(proxies[9]),
  })

  try {
    // Load markets for both exchanges
    await Promise.all([exchangeA.loadMarkets(), exchangeKucoinSpot.loadMarkets(), exchangeKucoinFuture.loadMarkets()]);
    console.log("Loading markets...");

    // Fetch symbols
    const exASpotSymbols = Object.keys(exchangeA.markets).filter(
      (symbol) => symbol.endsWith('/USDT') && !symbol.includes(':')
    );
    const exAFutureSymbols = Object.keys(exchangeA.markets).filter(
      (symbol) => symbol.includes('/USDT:')
    );
    const exKucoinSpotSymbols = Object.keys(exchangeKucoinSpot.markets).filter(
      (symbol) => symbol.endsWith('/USDT') && !symbol.includes(':')
    );
    const exKucoinFutureSymbols = Object.keys(exchangeKucoinFuture.markets).filter(
      (symbol) => symbol.includes('/USDT:')
    );

    // Identify common trading pairs
    const PairsExASExAF = exASpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futureSymbol);
    });
    const PairsExASExBF = exASpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exKucoinFutureSymbols.includes(futureSymbol);
    });
    const PairsExBSExBF = exKucoinSpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exKucoinFutureSymbols.includes(futureSymbol);
    });
    const PairsExBSExAF = exKucoinSpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futureSymbol);
    });
    const PairsExAFExBF = _.intersection(exAFutureSymbols, exKucoinFutureSymbols);

    // Display common trading pairs
    console.log(`Number of common trading pairs (${exA}-spot / ${exA}-futures): ${PairsExASExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-spot / Kucoin-futures): ${PairsExASExBF.length}`);
    console.log(`Number of common trading pairs (Kucoin-spot / Kucoin-futures): ${PairsExBSExBF.length}`);
    console.log(`Number of common trading pairs (Kucoin-spot / ${exA}-futures): ${PairsExBSExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-futures / Kucoin-futures): ${PairsExAFExBF.length}`);

    // Split each array into 10 parts
    const numParts = 10;
    const PairsExAFExBFChunks = chunkArray(PairsExAFExBF, numParts);
    const PairsExASExAFChunks = chunkArray(PairsExASExAF, numParts);
    const PairsExASExBFChunks = chunkArray(PairsExASExBF, numParts);
    const PairsExBSExBFChunks = chunkArray(PairsExBSExBF, numParts);
    const PairsExBSExAFChunks = chunkArray(PairsExBSExAF, numParts);

    // Create pairsToProcess with chunks
    const pairsToProcess = [];
    for (let i = 0; i < numParts; i++) {
      pairsToProcess.push([
        { pairs: PairsExAFExBFChunks[i], exchangeA: exchangeA, exchangeB: exchangeKucoinFuture, exchangeAName: `${exA}-FUTURE`, exchangeBName: `Kucoin-FUTURE` },
        { pairs: PairsExASExAFChunks[i], exchangeA: exchangeA, exchangeB: exchangeA, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exA}-FUTURE` },
        { pairs: PairsExBSExBFChunks[i], exchangeA: exchangeKucoinSpot, exchangeB: exchangeKucoinFuture, exchangeAName: `Kucoin-SPOT`, exchangeBName: `Kucoin-FUTURE` },
        { pairs: PairsExASExBFChunks[i], exchangeA: exchangeA, exchangeB: exchangeKucoinFuture, exchangeAName: `${exA}-SPOT`, exchangeBName: `Kucoin-FUTURE` },
        { pairs: PairsExBSExAFChunks[i], exchangeA: exchangeKucoinSpot, exchangeB: exchangeA, exchangeAName: `Kucoin-SPOT`, exchangeBName: `${exA}-FUTURE` }
      ]);
    }

    // Process all pairs
    await Promise.all(
      proxies.map(async (proxy, proxyIndex) => {
        await Promise.all(
          pairsToProcess[proxyIndex].map(async ({ pairs, exchangeA, exchangeB, exchangeAName, exchangeBName }) => {
            exchangeFlag[exchangeAName.split('-')[0]] = true;
            const batches = chunkify(pairs, 40);
            for (const batch of batches) {
              let batchA, batchB;
              await Promise.all([
                batchA = await filterPerDayVolume(exchangeA, batch),
                batchB = await filterPerDayVolume(exchangeB, batch)
              ]);

              if (batchA.length === 0 || batchB.length === 0) continue;

              let orderBooksA, orderBooksB;
              await Promise.all([
                orderBooksA = await fetchOrderBooks(exchangeA, batchA, exchangeAName, exchangeBName, proxy),
                orderBooksB = await fetchOrderBooks(exchangeB, batchB, exchangeBName, exchangeAName, proxy)
              ])
              const profits = calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName);              

              for (const profit of profits) {
                if (shouldNotifyOpportunity(profit.pair, profit.spread)) {
                  const message = 
                    `Coin: ${profit.pair}\n` +
                    `Direction: ${profit.direction}\n` +
                    `Spread: ${profit.spread.toFixed(2)}%\n` +
                    `Buy Price: ${profit.buyPrice}\n` +
                    `Sell Price: ${profit.sellPrice}\n` +
                    `Max Volume: ${profit.maxVolume.toFixed(2)}\n` +
                    `Potential Profit: ${profit.profit.toFixed(2)} USDT\n` +
                    `Links:\n` +
                    `- ${exchangeAName}: ${EXCHANGE_URLS[exchangeAName].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `- ${exchangeBName}: ${EXCHANGE_URLS[exchangeBName].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `------------------------------------------`
                  ;
                  await bot.sendMessage(TELEGRAM_CHAT_ID, message, { disable_web_page_preview: true });
                  updateOpportunityBuffer(profit.pair, profit.spread);
                }
              }
            }
          })
        );
      })
    );
  } catch (err) {
    console.error(`An error occurred in arbitrage check for ${exA} and ${exB}:`, err.message);
  }
}

async function executeArbitrageCheck(exA, exB) {
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });
  const exchangeA = exchanges[exA];
  const exchangeB = exchanges[exB];

  try {
    // Load markets for both exchanges
    await Promise.all([exchangeA.loadMarkets(), exchangeB.loadMarkets()]);
    
    console.log("Loading markets...");

    // Fetch symbols
    const exASpotSymbols = Object.keys(exchangeA.markets).filter(
      (symbol) => symbol.endsWith('/USDT') && !symbol.includes(':')
    );
    const exAFutureSymbols = Object.keys(exchangeA.markets).filter(
      (symbol) => symbol.includes('/USDT:')
    );
    const exBSpotSymbols = Object.keys(exchangeB.markets).filter(
      (symbol) => symbol.endsWith('/USDT') && !symbol.includes(':')
    );
    const exBFutureSymbols = Object.keys(exchangeB.markets).filter(
      (symbol) => symbol.includes('/USDT:')
    );

    // Identify common trading pairs
    const PairsExASExAF = exASpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futureSymbol);
    });
    const PairsExASExBF = exASpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exBFutureSymbols.includes(futureSymbol);
    });
    const PairsExBSExBF = exBSpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exBFutureSymbols.includes(futureSymbol);
    });
    const PairsExBSExAF = exBSpotSymbols.filter((spotSymbol) => {
      const futureSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futureSymbol);
    });
    const PairsExAFExBF = _.intersection(exAFutureSymbols, exBFutureSymbols);

    // Display common trading pairs
    console.log(`Number of common trading pairs (${exA}-spot / ${exA}-futures): ${PairsExASExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-spot / ${exB}-futures): ${PairsExASExBF.length}`);
    console.log(`Number of common trading pairs (${exB}-spot / ${exB}-futures): ${PairsExBSExBF.length}`);
    console.log(`Number of common trading pairs (${exB}-spot / ${exA}-futures): ${PairsExBSExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-futures / ${exB}-futures): ${PairsExAFExBF.length}`);

    // Split each array into 10 parts
    const numParts = 10;
    const PairsExAFExBFChunks = chunkArray(PairsExAFExBF, numParts);
    const PairsExASExAFChunks = chunkArray(PairsExASExAF, numParts);
    const PairsExASExBFChunks = chunkArray(PairsExASExBF, numParts);
    const PairsExBSExBFChunks = chunkArray(PairsExBSExBF, numParts);
    const PairsExBSExAFChunks = chunkArray(PairsExBSExAF, numParts);

    // Create pairsToProcess with chunks
    const pairsToProcess = [];
    for (let i = 0; i < numParts; i++) {
      pairsToProcess.push([
        { pairs: PairsExAFExBFChunks[i], exchangeA: exchangeA, exchangeB: exchangeB, exchangeAName: `${exA}-FUTURE`, exchangeBName: `${exB}-FUTURE` },
        { pairs: PairsExASExAFChunks[i], exchangeA: exchangeA, exchangeB: exchangeA, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exA}-FUTURE` },
        { pairs: PairsExBSExBFChunks[i], exchangeA: exchangeB, exchangeB: exchangeB, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exB}-FUTURE` },
        { pairs: PairsExASExBFChunks[i], exchangeA: exchangeA, exchangeB: exchangeB, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exB}-FUTURE` },
        { pairs: PairsExBSExAFChunks[i], exchangeA: exchangeB, exchangeB: exchangeA, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exA}-FUTURE` }
      ]);
    }

    // Process all pairs
    await Promise.all(
      proxies.map(async (proxy, proxyIndex) => {
        await Promise.all(
          pairsToProcess[proxyIndex].map(async ({ pairs, exchangeA, exchangeB, exchangeAName, exchangeBName }) => {
            if (exchangeAName.split('-')[0] === exchangeBName.split('-')[0]) {
              if (exchangeFlag[exchangeAName.split('-')[0]]) {
                return;
              } else {
                exchangeFlag[exchangeAName.split('-')[0]] = true;
              }
            }
            const batches = chunkify(pairs, 40);
            for (const batch of batches) {
              let batchA, batchB;
              await Promise.all([
                batchA = await filterPerDayVolume(exchangeA, batch),
                batchB = await filterPerDayVolume(exchangeB, batch)
              ]);

              if (batchA.length === 0 || batchB.length === 0) continue;

              let orderBooksA, orderBooksB;
              await Promise.all([
                orderBooksA = await fetchOrderBooks(exchangeA, batchA, exchangeAName, exchangeBName, proxy),
                orderBooksB = await fetchOrderBooks(exchangeB, batchB, exchangeBName, exchangeAName, proxy)
              ]);              
              const profits = calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName);

              for (const profit of profits) {
                if (shouldNotifyOpportunity(profit.pair, profit.spread)) {
                  const message = 
                    `Coin: ${profit.pair}\n` +
                    `Direction: ${profit.direction}\n` +
                    `Spread: ${profit.spread.toFixed(2)}%\n` +
                    `Buy Price: ${profit.buyPrice}\n` +
                    `Sell Price: ${profit.sellPrice}\n` +
                    `Max Volume: ${profit.maxVolume.toFixed(2)}\n` +
                    `Potential Profit: ${profit.profit.toFixed(2)} USDT\n` +
                    `Links:\n` +
                    `- ${exchangeAName}: ${EXCHANGE_URLS[exchangeAName].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `- ${exchangeBName}: ${EXCHANGE_URLS[exchangeBName].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `------------------------------------------`
                  ;
                  await bot.sendMessage(TELEGRAM_CHAT_ID, message, { disable_web_page_preview: true });
                  updateOpportunityBuffer(profit.pair, profit.spread);
                }
              }
            }
          })
        );
      })
    );
  } catch (err) {
    console.error(`An error occurred in arbitrage check for ${exA} and ${exB}:`, err.message);
  }
}

(async function main() {
  console.log(`Starting the bot...`);
  while (true) {
    try {
      const scriptStartTime = Date.now();

      // Calculations between Kucoin and other exchanges
      const arbitrageKucoinPromises = [];
      for (let i = 0; i < EXCHANGES.length; i++) {
        arbitrageKucoinPromises.push(executeArbitrageKucoinCheck(EXCHANGES[i]));
      }
      await Promise.all(arbitrageKucoinPromises);

      // Calculations between other exchanges
      for (let i = 0; i < EXCHANGES.length; i++) {
        const arbitragePromises = [];
        for (let j = i + 1; j < EXCHANGES.length; j++) {
          arbitragePromises.push(executeArbitrageCheck(EXCHANGES[i], EXCHANGES[j]));
        }
        await Promise.all(arbitragePromises);
      }
      console.log(`Total calculation completed in ${(Date.now() - scriptStartTime) / 1000}s`);
      orderBookCache = {};
    } catch (err) {
      console.error("Error in main loop:", err.message);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
})();
