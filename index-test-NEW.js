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
  EXCHANGES
} from "./config.js";

const exchanges = Object.fromEntries(
  EXCHANGES.map((exchangeName, index) => [
    exchangeName,
    new ccxt[exchangeName.toLowerCase()]({
      rateLimit: 200,
      agent: new SocksProxyAgent(proxies[index % proxies.length]),
    }),
  ])
);
const opportunityBuffer = {};
const orderBookCache = {};

process.setMaxListeners(MAX_PROCESS_COUNT);

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
        console.error('Failed after retries:', err.message);
        throw err; // Propagate error if retries exhausted
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

// Fetch order books with caching
async function fetchOrderBooksWithCache(exchange, pair, exchangeBuyName, exchangeSellName) {
  const cacheKey = `${exchange.id}:${pair}`;
  if (orderBookCache[cacheKey]) {
    return orderBookCache[cacheKey];
  }
  const orderBook = await fetchWithRetry(() => {
    if (
      exchange.symbols.includes(pair) &&
      exchangeBuyName.includes('FUTURE') &&
      exchangeSellName.includes('SPOT')
    ) {
      exchange.fetchOrderBook(pair.replace('/USDT', '/USDT:USDT'))
    } else if (exchange.symbols.includes(pair)) {
      exchange.fetchOrderBook(pair);
    }
  }, 3, 1000);
  orderBookCache[cacheKey] = orderBook; // Save to cache
  return orderBook;
}

async function fetchOrderBooks(exchange, pairs, exchangeBuyName, exchangeSellName, proxy) {
  const orderBooks = {};
  exchange.agent = new SocksProxyAgent(proxy);

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

function calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName) {
  const profits = [];

  for (const pair in orderBooksA) {
    if (orderBooksB[pair]) {
      const asksA = orderBooksA[pair]?.asks[0];
      const bidsB = orderBooksB[pair]?.bids[0];
      const bidsA = orderBooksA[pair]?.bids[0];
      const asksB = orderBooksB[pair]?.asks[0];

      if (!asksA || !bidsB || !bidsA || !asksB) continue;

      const spreadAB = ((bidsB[0] - asksA[0]) / asksA[0]) * 100;
      const spreadBA = ((bidsA[0] - asksB[0]) / asksB[0]) * 100;

      const buyVolumeA = asksA[1];
      const sellVolumeB = bidsB[1];
      const buyVolumeB = asksB[1];
      const sellVolumeA = bidsA[1];

      const maxVolumeAB = Math.min(buyVolumeA, sellVolumeB);
      const maxVolumeBA = Math.min(buyVolumeB, sellVolumeA);

      const profitAB = maxVolumeAB * spreadAB / 100;
      const profitBA = maxVolumeBA * spreadBA / 100;

      if (spreadAB >= MIN_SPREAD && maxVolumeAB >= MIN_VOLUME) {
        profits.push({
          pair,
          direction: `${exchangeAName}->${exchangeBName}`,
          exchangeA: exchangeAName,
          exchangeB: exchangeBName,
          spread: isNaN(spreadAB) ? 0 : spreadAB,
          buyPrice: asksA[0],
          sellPrice: bidsB[0],
          maxVolume: maxVolumeAB,
          profit: isNaN(profitAB) ? 0 : profitAB
        });
      }

      if (spreadBA >= MIN_SPREAD && maxVolumeBA >= MIN_VOLUME) {
        profits.push({
          pair,
          direction: `${exchangeBName}->${exchangeAName}`,
          exchangeA: exchangeBName,
          exchangeB: exchangeAName,
          spread: isNaN(spreadBA) ? 0 : spreadBA,
          buyPrice: asksB[0] || 0,
          sellPrice: bidsA[0] || 0,
          maxVolume: maxVolumeBA,
          profit: isNaN(profitBA) ? 0 : profitBA
        });
      }
    }
  }
  return profits;
}

function filterActivePairs(orderBooks) {
  return Object.keys(orderBooks).reduce((filtered, pair) => {
    if (orderBooks[pair]?.asks?.length && orderBooks[pair]?.bids?.length) {
      filtered[pair] = orderBooks[pair];
    }
    return filtered;
  }, {});
}

async function filterPerDayVolume(exchange, pairs) {
  try {
    const markets = await exchange.loadMarkets();
    const validPairs = pairs.filter((pair) => markets[pair]);

    if (validPairs.length === 0) return [];

    let tickers;
    try {
      tickers = await exchange.fetchTickers(validPairs);
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

async function executeArbitrageCheck(exA, exB) {
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });
  const exchangePlateA = exchanges[exA];
  const exchangePlateB = exchanges[exB];

  try {
    await Promise.all([exchangePlateA.loadMarkets(), exchangePlateB.loadMarkets()]);
    const exASymbols = Object.keys(exchangePlateA.markets);
    const exBSymbols = Object.keys(exchangePlateB.markets);

    // Filter futures pairs
    console.log("Loading markets...");
    const marketStartTime = Date.now();
    const exASpotSymbols = exASymbols.filter(symbol => symbol.endsWith('/USDT') && !symbol.includes(':'));
    const exAFutureSymbols = exASymbols.filter((symbol) => symbol.includes('/USDT:'));
    const exBSpotSymbols = exBSymbols.filter((symbol) => symbol.endsWith('/USDT'));
    const exBFutureSymbols = exBSymbols.filter((symbol) => symbol.includes('/USDT:'));

    const PairsExASExAF = exASpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futuresSymbol);
    });
    const PairsExASExBF = exASpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exBFutureSymbols.includes(futuresSymbol);
    });
    const PairsExBSExBF = exBSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exBFutureSymbols.includes(futuresSymbol);
    });
    const PairsExBSExAF = exBSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return exAFutureSymbols.includes(futuresSymbol);
    });
    const PairsExAFExBF = _.intersection(exAFutureSymbols, exBFutureSymbols);

    console.log(`Markets loaded in ${(Date.now() - marketStartTime) / 1000}s`);
    console.log(`Number of common trading pairs (${exA}-spot / ${exA}-futures): ${PairsExASExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-spot / ${exB}-futures): ${PairsExASExBF.length}`);
    console.log(`Number of common trading pairs (${exB}-spot / ${exB}-futures): ${PairsExBSExBF.length}`);
    console.log(`Number of common trading pairs (${exB}-spot / ${exA}-futures): ${PairsExBSExAF.length}`);
    console.log(`Number of common trading pairs (${exA}-futures / ${exB}-futures): ${PairsExAFExBF.length}`);

    function chunkArray(array, numParts) {
      const chunkSize = Math.ceil(array.length / numParts);
      return Array.from({ length: numParts }, (_, index) =>
        array.slice(index * chunkSize, (index + 1) * chunkSize)
      );
    }
    
    // Split each array into 10 parts
    const numParts = 2;
    const PairsExAFExBFChunks = chunkArray(PairsExAFExBF, numParts);
    const PairsExASExAFChunks = chunkArray(PairsExASExAF, numParts);
    const PairsExASExBFChunks = chunkArray(PairsExASExBF, numParts);
    const PairsExBSExBFChunks = chunkArray(PairsExBSExBF, numParts);
    const PairsExBSExAFChunks = chunkArray(PairsExBSExAF, numParts);
    
    // Create pairsToProcess with chunks
    const pairsToProcess = [];    
    for (let i = 0; i < numParts; i++) {
      pairsToProcess.push([
        { pairs: PairsExAFExBFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateB, exchangeAName: `${exA}-FUTURE`, exchangeBName: `${exB}-FUTURE`, proxy: proxies[i * 5] },
        { pairs: PairsExASExAFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateA, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exA}-FUTURE`, proxy: proxies[i * 5 + 1] },
        { pairs: PairsExASExBFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateB, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exB}-FUTURE`, proxy: proxies[i * 5 + 2] },
        { pairs: PairsExBSExBFChunks[i], exchangeA: exchangePlateB, exchangeB: exchangePlateB, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exB}-FUTURE`, proxy: proxies[i * 5 + 3] },
        { pairs: PairsExBSExAFChunks[i], exchangeA: exchangePlateB, exchangeB: exchangePlateA, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exA}-FUTURE`, proxy: proxies[i * 5 + 4] }
      ]);
    }

    const fetchStartTime = Date.now();
    await Promise.all(
      pairsToProcess.map(async (pairsToProcessBunch) => {
        await Promise.all(
          pairsToProcessBunch.map(async ({ pairs, exchangeA, exchangeB, exchangeAName, exchangeBName, proxy }) => {
            const batches = chunkify(pairs, 2);
            for (const batch of batches) {
              let orderBooksA = [];
              let orderBooksB = [];
    
              // const batchA = await filterPerDayVolume(exchangeA, batch);
              // const batchB = await filterPerDayVolume(exchangeB, batch);
              
              const fetchStartTime = Date.now();
              await Promise.all([
                orderBooksA = filterActivePairs(await fetchOrderBooks(exchangeA, batch, exchangeAName, exchangeBName, proxy)),
                orderBooksB = filterActivePairs(await fetchOrderBooks(exchangeB, batch, exchangeBName, exchangeAName, proxy))
              ])
              console.log(`Order books fetched and spreads calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
          
              const profits = calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName);
    
              for (const { pair, spread, direction, exchangeA, exchangeB, buyPrice, sellPrice, maxVolume, profit } of profits) {
                if (shouldNotifyOpportunity(pair, spread)) {
                  console.log(`Alert sent for ${pair}: Spread: ${spread}%, Profit: ${profit}`);
                  const message =
                    `Coin: ${pair}\n` +
                    `Direction: ${direction}\n` +
                    `Spread: ${spread.toFixed(2)}%\n` +
                    `Buy Price: ${buyPrice}\n` +
                    `Sell Price: ${sellPrice}\n` +
                    `Max Volume: ${maxVolume.toFixed(2)}\n` +
                    `Potential Profit: ${profit.toFixed(2)} USDT\n` +
                    `Links:\n` +
                    `- ${exchangeA}: ${EXCHANGE_URLS[exchangeA].replace('AAA', pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `- ${exchangeB}: ${EXCHANGE_URLS[exchangeB].replace('AAA', pair.split('/')[0].toUpperCase()).replace(':USDT', '')}\n` +
                    `------------------------------------------`
                  ;
                  await bot.sendMessage(TELEGRAM_CHAT_ID, message, {disable_web_page_preview: true});
                  updateOpportunityBuffer(pair, spread);
                }
              }
            }
          })
        )
      })
    )
    console.log(`total calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
  } catch (err) {
    console.error("An error occurred:", err.message);
  }
};

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
      const scriptStartTime = Date.now();
      await Promise.all(arbitragePromises);
      console.log(`Cycle completed in ${(Date.now() - scriptStartTime) / 1000}s`);
    } catch (err) {
      console.error("Error in main loop:", err.message);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));

    process.on('SIGINT', () => {
      console.log("\nStopping the bot...");
      process.exit(0);
    });
  }
})();