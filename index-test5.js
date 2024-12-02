import ccxt from 'ccxt';
import TelegramBot from 'node-telegram-bot-api';
import _ from 'lodash';
import {
  TELEGRAM_API_KEY,
  TELEGRAM_CHAT_ID,
  MIN_SPREAD,
  MIN_VOLUME,
  BUFFER_EXPIRY_TIME,
  ARBITRAGE_PERCENTAGE_THRESHOLD,
  proxies,
  MAX_PROCESS_COUNT
} from "./config.js";
const exchange1 = 'Gate';
const exchange2 = 'Mexc';
const opportunityBuffer = {};
const BATCH_SIZE = Math.min(Math.max(proxies.length, 10), MAX_PROCESS_COUNT);
process.setMaxListeners(MAX_PROCESS_COUNT);

function chunkify(array, size) {
  return _.chunk(array, size);
}

async function fetchOrderBooks(exchange, pairs, exchangeBuyName, exchangeSellName, proxy) {
  const orderBooks = {};
  await Promise.all(
    pairs.map(async (pair) => {
      try {
        let orderBook = {};
        exchange.socksProxy = proxy;
        if (exchangeBuyName.includes('FUTURE') && exchangeSellName.includes('SPOT')) {
          orderBook = await exchange.fetchOrderBook(pair.replace('/USDT', '/USDT:USDT'));
        } else {
          orderBook = await exchange.fetchOrderBook(pair);
        }
        orderBooks[pair] = orderBook;
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

      console.log(bidsA);
      console.log(asksB);

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
  const exchangePlateA = new ccxt.gateio();
  const exchangePlateB = new ccxt.mexc();

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
    const numParts = 5;
    const PairsExAFExBFChunks = chunkArray(PairsExAFExBF, numParts);
    const PairsExASExAFChunks = chunkArray(PairsExASExAF, numParts);
    const PairsExASExBFChunks = chunkArray(PairsExASExBF, numParts);
    const PairsExBSExBFChunks = chunkArray(PairsExBSExBF, numParts);
    const PairsExBSExAFChunks = chunkArray(PairsExBSExAF, numParts);
    
    // Create pairsToProcess with chunks
    const pairsToProcess = [];    
    for (let i = 0; i < numParts; i++) {
      pairsToProcess.push([
        { pairs: PairsExAFExBFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateB, exchangeAName: `${exA}-FUTURE`, exchangeBName: `${exB}-FUTURE` },
        // { pairs: PairsExASExAFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateA, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exA}-FUTURE` },
        // { pairs: PairsExASExBFChunks[i], exchangeA: exchangePlateA, exchangeB: exchangePlateB, exchangeAName: `${exA}-SPOT`, exchangeBName: `${exB}-FUTURE` },
        // { pairs: PairsExBSExBFChunks[i], exchangeA: exchangePlateB, exchangeB: exchangePlateB, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exB}-FUTURE` },
        // { pairs: PairsExBSExAFChunks[i], exchangeA: exchangePlateB, exchangeB: exchangePlateA, exchangeAName: `${exB}-SPOT`, exchangeBName: `${exA}-FUTURE` }
      ]);
    }

    const aaaa = [1, 2, 3, 4, 5]

    const fetchStartTime = Date.now();
    await Promise.all(
      aaaa.map(async (_, index) => {
        for (const { pairs, exchangeA, exchangeB, exchangeAName, exchangeBName } of pairsToProcess[index]) {
          const batches = chunkify(pairs, 50);
          for (const batch of batches) {
            let orderBooksA = [];
            let orderBooksB = [];
      
            const fetchStartTime = Date.now();
            await Promise.all([
              orderBooksA = filterActivePairs(await fetchOrderBooks(exchangeA, batch, exchangeAName, exchangeBName, proxies[(index * 2) % proxies.length])),
              orderBooksB = filterActivePairs(await fetchOrderBooks(exchangeB, batch, exchangeBName, exchangeAName, proxies[(index * 2 + 1) % proxies.length]))
            ])
            console.log(`Order books fetched and spreads calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
        
            const profits = calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName);

            for (const { pair, spread, direction, buyPrice, sellPrice, maxVolume, profit } of profits) {
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
                  `- ${exA}: https://www.gate.io/futures/USDT/${pair.replace('/', '_').replace(':USDT', '')}\n` + // need to be modified
                  `- ${exB}: https://futures.mexc.com/exchange/${pair.replace('/', '_').replace(':USDT', '')}\n` + // need to be modified
                  `------------------------------------------`
                ;
                await bot.sendMessage(TELEGRAM_CHAT_ID, message, {disable_web_page_preview: true});
                updateOpportunityBuffer(pair, spread);
              }
            }
          }
        }
        console.log(`total calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
      })
    )
  } catch (err) {
    console.error("An error occurred:", err.message);
  }
};

(async function main() {
  console.log("Starting the bot...");
  
  while (true) {
    await executeArbitrageCheck(exchange1, exchange2);
    await new Promise((resolve) => setTimeout(resolve, 1000));

    process.on('SIGINT', () => {
      console.log("\nStopping the bot...");
      process.exit(0);
    });
  }
})();