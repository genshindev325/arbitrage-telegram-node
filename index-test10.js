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
const orderBookCache = {}; // Cache for order books

const exchangeFlag = {};
for (const exchangeName of EXCHANGES) {
  exchangeFlag[exchangeName] = false;
}

// Initialize exchanges
const exchanges = Object.fromEntries(
  EXCHANGES.map((exchangeName, index) => [
    exchangeName,
    new ccxt[exchangeName.toLowerCase()]({
      rateLimit: 200,
      agent: new SocksProxyAgent(proxies[index % proxies.length]),
    }),
  ])
);

process.setMaxListeners(MAX_PROCESS_COUNT);

// Helper functions
function chunkify(array, size) {
  return _.chunk(array, size);
}

async function fetchWithRetry(fn, retries = 3, delay = 1000) {
  let attempts = 0;
  while (attempts < retries) {
    try {
      return await fn();
    } catch (err) {
      attempts++;
      if (attempts >= retries) {
        throw err; // Propagate error if retries exhausted
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

// Fetch order books with caching
async function fetchOrderBooksWithCache(exchange, pair) {
  const cacheKey = `${exchange.id}:${pair}`;
  if (orderBookCache[cacheKey]) {
    return orderBookCache[cacheKey];
  }
  const orderBook = await fetchWithRetry(() => exchange.fetchOrderBook(pair), 3, 1000);
  orderBookCache[cacheKey] = orderBook; // Save to cache
  return orderBook;
}

async function fetchOrderBooks(exchange, pairs, proxy) {
  const orderBooks = {};
  exchange.agent = new SocksProxyAgent(proxy);

  for (const pair of pairs) {
    try {
      orderBooks[pair] = await fetchOrderBooksWithCache(exchange, pair);
    } catch (err) {
      console.error(`Error fetching order book for ${pair}:`, err.message);
    }
  }
  return orderBooks;
}

function shouldNotifyOpportunity(pair, newPercentage) {
  const now = Date.now();

  if (!opportunityBuffer[pair]) return true;

  const { lastPercentage, lastTimestamp } = opportunityBuffer[pair];
  const timeElapsed = now - lastTimestamp;
  const percentageChange = newPercentage - lastPercentage;

  return timeElapsed > BUFFER_EXPIRY_TIME || percentageChange >= ARBITRAGE_PERCENTAGE_THRESHOLD;
}

function updateOpportunityBuffer(pair, percentage) {
  opportunityBuffer[pair] = {
    lastPercentage: percentage,
    lastTimestamp: Date.now(),
  };
}

async function filterPerDayVolume(exchange, pairs) {
  try {
    // Step 1: Fetch available markets on the exchange
    const markets = await exchange.loadMarkets();

    // Step 2: Filter out pairs that are not supported by the exchange
    const validPairs = pairs.filter(pair => markets[pair]);

    if (validPairs.length === 0) {
      return [];
    }

    // Fetch tickers for all pairs
    let tickers;
    try {
      tickers = await exchange.fetchTickers(validPairs);
    } catch (tickerError) {
      return []; // Return an empty array if fetching tickers fails
    }

    // Filter pairs with volume greater than MIN_24_VOLUME
    const filteredPairs = validPairs.filter(pair => {
      const ticker = tickers[pair];
      return ticker && ticker.baseVolume && ticker.baseVolume > MIN_24_VOLUME;
    });

    return filteredPairs;
  } catch (error) {
    console.error(`Error in filterPerDayVolume for ${exchange.name}:`, error.message);
    return []; // Return an empty array on error
  }
}


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

async function executeArbitrageCheck(exA, exB) {
  const exchangeA = exchanges[exA];
  const exchangeB = exchanges[exB];
  try {
    await Promise.all([exchangeA.loadMarkets(), exchangeB.loadMarkets()]);

    const pairs = _.intersection(
      Object.keys(exchangeA.markets),
      Object.keys(exchangeB.markets)
    ).filter((pair) => pair.endsWith('/USDT'));

    const batchA = await filterPerDayVolume(exchangeA, pairs);
    const batchB = await filterPerDayVolume(exchangeB, pairs);
    if (batchA.length === 0 || batchB.length === 0) return;

    const orderBooksA = await fetchOrderBooks(exchangeA, batchA, proxies[0]);
    const orderBooksB = await fetchOrderBooks(exchangeB, batchB, proxies[1]);

    const profits = calculateProfit(orderBooksA, orderBooksB, exA, exB);
    const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });

    for (const profit of profits) {
      if (shouldNotifyOpportunity(profit.pair, profit.spread)) {
        const message = `
          Coin: ${profit.pair}
          Direction: ${profit.direction}
          Spread: ${profit.spread.toFixed(2)}%
          Buy Price: ${profit.buyPrice}
          Sell Price: ${profit.sellPrice}
          Max Volume: ${profit.maxVolume.toFixed(2)}
          Potential Profit: ${profit.profit.toFixed(2)} USDT
          Links:
          - ${exchangeA}: ${EXCHANGE_URLS[exchangeA].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}
          - ${exchangeB}: ${EXCHANGE_URLS[exchangeB].replace('AAA', profit.pair.split('/')[0].toUpperCase()).replace(':USDT', '')}
          `;
        await bot.sendMessage(TELEGRAM_CHAT_ID, message, { disable_web_page_preview: true });
        updateOpportunityBuffer(profit.pair, profit.spread);
      }
    }
  } catch (err) {
    console.error(`Error in arbitrage check for ${exA} and ${exB}:`, err.message);
  }
}

(async function main() {
  console.log("Starting the bot...");
  while (true) {
    try {
      const arbitragePromises = [];
      for (let i = 0; i < EXCHANGES.length; i++) {
        for (let j = i + 1; j < EXCHANGES.length; j++) {
          arbitragePromises.push(executeArbitrageCheck(EXCHANGES[i], EXCHANGES[j]));
        }
      }
      await Promise.all(arbitragePromises);
      console.log("Cycle complete.");
    } catch (err) {
      console.error("Error in main loop:", err.message);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
})();
