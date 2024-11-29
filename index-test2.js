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
  MAX_PROCESS_COUNT
} from "./config.js";

const opportunityBuffer = {};
// let currentProxyIndex = -1;
// const BATCH_SIZE = Math.min(Math.max(proxies.length, 10), MAX_PROCESS_COUNT);
const BATCH_SIZE = 10;
process.setMaxListeners(MAX_PROCESS_COUNT);

function chunkify(array, size) {
  return _.chunk(array, size);
}

// function getNextProxy() {
//   currentProxyIndex = (currentProxyIndex + 1) % proxies.length;
//   return proxies[currentProxyIndex];
// }

// { pairs: PairsGateSGateF, exchangeA: gate, exchangeB: gate, exchangeAName: 'GATE-SPOT', exchangeBName: 'GATE-FUTURE' },
// { pairs: PairsGateSMexcF, exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-SPOT', exchangeBName: 'MEXC-FUTURE' },
// { pairs: PairsMexcSMexcF, exchangeA: mexc, exchangeB: mexc, exchangeAName: 'MEX-SPOT, exchangeBName: 'MEXC-FUTURE' },
// { pairs: PairsMexcSGateF, exchangeA: mexc, exchangeB: gate, exchangeAName: 'MEXC-SPOT', exchangeBName: 'GATE-FUTURE' },
// { pairs: PairsGateFMexcF, exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-FUTURE', exchangeBName: 'MEXC-FUTURE' }

// orderBooksA = filterActivePairs(await fetchOrderBooks(exchangeA, batch(pairs), exchangeB, exchangeAName)),
// orderBooksB = filterActivePairs(await fetchOrderBooks(exchangeB, batch(pairs), exchangeA, exchangeAName))
// Function to add delay (in ms)
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Retry fetch logic
async function fetchWithRetry(fn, retries = 3, delayTime = 1000) {
  let lastError;
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      console.error(`Attempt ${i + 1} failed: ${err.message}`);
      if (i < retries - 1) {
        console.log(`Retrying after ${delayTime}ms...`);
        await delay(delayTime);
      }
    }
  }
  throw lastError; // If all retries fail, throw the last error
}

async function fetchOrderBooks(exchange, pairs, exchangeBuyName, exchangeSellName, proxy) {
  const orderBooks = {};
  
  // await Promise.all(
  //   pairs.map(async (pair) => {
  for (const pair of pairs) {
    try {
      let orderBook = {};
      exchange.socksProxy = proxy;

      // Check if FUTURE/USDT:USDT logic needs to be applied
      if (exchangeBuyName.includes('FUTURE') && exchangeSellName.includes('SPOT')) {
        orderBook = await fetchWithRetry(() => exchange.fetchOrderBook(pair.replace('/USDT', '/USDT:USDT')));
      } else {
        orderBook = await fetchWithRetry(() => exchange.fetchOrderBook(pair));
      }
      
      // Store the fetched order book in the orderBooks object
      orderBooks[pair] = orderBook;

      // Introduce a delay to avoid rate-limiting
      await delay(10); // Adjust this delay (1000ms = 1 second)
    } catch (err) {
      console.error(`Error fetching order book for ${pair}:`, err.message);
    }
  }
  // }))

  return orderBooks;
}
// async function fetchOrderBooks(exchange, pairs, exchangeBuyName, exchangeSellName, proxy) {
//   const orderBooks = {};
//   await Promise.all(
//     pairs.map(async (pair) => {
//       try {
//         let orderBook = {};
//         exchange.socksProxy = proxy;
//         if (exchangeBuyName.includes('FUTURE') && exchangeSellName.includes('SPOT')) {
//           orderBook = await exchange.fetchOrderBook(pair.replace('/USDT', '/USDT:USDT'));
//         } else {
//           orderBook = await exchange.fetchOrderBook(pair);
//         }
//         orderBooks[pair] = orderBook;
//       } catch (err) {
//         console.error(`Error fetching order book for ${pair}:`, err.message);
//       }
//     })
//   );
//   return orderBooks;
// }

// { pairs: PairsGateSGateF, exchangeA: gate, exchangeB: gate, exchangeAName: 'GATE-SPOT', exchangeBName: 'GATE-FUTURE' },
// { pairs: PairsGateSMexcF, exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-SPOT', exchangeBName: 'MEXC-FUTURE' },
// { pairs: PairsMexcSMexcF, exchangeA: mexc, exchangeB: mexc, exchangeAName: 'MEX-SPOT, exchangeBName: 'MEXC-FUTURE' },
// { pairs: PairsMexcSGateF, exchangeA: mexc, exchangeB: gate, exchangeAName: 'MEXC-SPOT', exchangeBName: 'GATE-FUTURE' },
// { pairs: PairsGateFMexcF, exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-FUTURE', exchangeBName: 'MEXC-FUTURE' }
function calculateProfit(orderBooksA, orderBooksB, exchangeAName, exchangeBName) {
  const profits = [];

  for (const pair in orderBooksA) {
    let comparePair = pair;
    if (exchangeAName.includes('SPOT') && exchangeBName.includes('FUTURE')) {
      comparePair = pair.replace('/USDT', '/USDT:USDT');
    } else if (exchangeAName.includes('FUTURE') && exchangeBName.includes('SPOT')) {
      comparePair = pair.replace(':USDT', '');
    }
    if (orderBooksB[comparePair]) {
      const asksA = orderBooksA[pair]?.asks[0];
      const bidsB = orderBooksB[comparePair]?.bids[0];
      const bidsA = orderBooksA[pair]?.bids[0];
      const asksB = orderBooksB[comparePair]?.asks[0];

      if (!asksA || !bidsB || !bidsA || !asksB) continue;

      const spreadAB = ((bidsB[0] - asksA[0]) / asksA[0]) * 100;
      const spreadBA = ((bidsA[0] - asksB[0]) / asksB[0]) * 100;

      const buyVolumeA = asksA[1];
      const sellVolumeB = bidsB[1];
      const buyVolumeB = asksB[1];
      const sellVolumeA = bidsA[1];

      const maxVolumeAB = Math.min(buyVolumeA, sellVolumeB);
      const maxVolumeBA = Math.min(buyVolumeB, sellVolumeA);

      const profitAB = maxVolumeAB * Math.abs(bidsB[0] - asksA[0]);
      const profitBA = maxVolumeBA * Math.abs(bidsA[0] - asksB[0]);

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

async function executeArbitrageCheck() {
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });
  const gate = new ccxt.gateio();
  const mexc = new ccxt.mexc();

  try {
    // await gate.loadMarkets({ options: { defaultType: 'future' } });
    // await mexc.loadMarkets({ options: { defaultType: 'future' } });
    await Promise.all([gate.loadMarkets(), mexc.loadMarkets()]);
    const gateSymbols = Object.keys(gate.markets);
    const mexcSymbols = Object.keys(mexc.markets);

    // Filter futures pairs
    console.log("Loading markets...");
    const marketStartTime = Date.now();
    const gateSpotSymbols = gateSymbols.filter(symbol => symbol.endsWith('/USDT') && !symbol.includes(':'));
    const gateFutureSymbols = gateSymbols.filter((symbol) => symbol.includes('/USDT:'));
    const mexcSpotSymbols = mexcSymbols.filter((symbol) => symbol.endsWith('/USDT'));
    const mexcFutureSymbols = mexcSymbols.filter((symbol) => symbol.includes('/USDT:'));

    const PairsGateSGateF = gateSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return gateFutureSymbols.includes(futuresSymbol);
    });
    const PairsGateSMexcF = gateSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return mexcFutureSymbols.includes(futuresSymbol);
    });
    const PairsMexcSMexcF = mexcSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return mexcFutureSymbols.includes(futuresSymbol);
    });
    const PairsMexcSGateF = mexcSpotSymbols.filter(spotSymbol => {
      const futuresSymbol = spotSymbol.replace('/USDT', '/USDT:USDT');
      return gateFutureSymbols.includes(futuresSymbol);
    });
    const PairsGateFMexcF = _.intersection(gateFutureSymbols, mexcFutureSymbols);

    console.log(`Markets loaded in ${(Date.now() - marketStartTime) / 1000}s`);
    console.log(`Number of common trading pairs (Gate-spot / Gate-futures): ${PairsGateSGateF.length}`);
    console.log(`Number of common trading pairs (Gate-spot / Mexc-futures): ${PairsGateSMexcF.length}`);
    console.log(`Number of common trading pairs (Mexc-spot / Mexc-futures): ${PairsMexcSMexcF.length}`);
    console.log(`Number of common trading pairs (Mexc-spot / Gate-futures): ${PairsMexcSGateF.length}`);
    console.log(`Number of common trading pairs (Gate-futures / Mexc-futures): ${PairsGateFMexcF.length}`);

    function chunkArray(array, numParts) {
      const chunkSize = Math.ceil(array.length / numParts);
      return Array.from({ length: numParts }, (_, index) =>
        array.slice(index * chunkSize, (index + 1) * chunkSize)
      );
    }
    
    // Split each array into 10 parts
    const numParts = 10;
    const pairsGateFMexcFChunks = chunkArray(PairsGateFMexcF, numParts);
    const pairsGateSGateFChunks = chunkArray(PairsGateSGateF, numParts);
    const pairsGateSMexcFChunks = chunkArray(PairsGateSMexcF, numParts);
    const pairsMexcSMexcFChunks = chunkArray(PairsMexcSMexcF, numParts);
    const pairsMexcSGateFChunks = chunkArray(PairsMexcSGateF, numParts);
    
    // Create pairsToProcess with chunks
    const pairsToProcess = [];
    
    for (let i = 0; i < numParts; i++) {
      pairsToProcess.push([
        // { pairs: pairsGateFMexcFChunks[i], exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-FUTURE', exchangeBName: 'MEXC-FUTURE' },
        { pairs: pairsGateSGateFChunks[i], exchangeA: gate, exchangeB: gate, exchangeAName: 'GATE-SPOT', exchangeBName: 'GATE-FUTURE' },
        // { pairs: pairsGateSMexcFChunks[i], exchangeA: gate, exchangeB: mexc, exchangeAName: 'GATE-SPOT', exchangeBName: 'MEXC-FUTURE' },
        // { pairs: pairsMexcSMexcFChunks[i], exchangeA: mexc, exchangeB: mexc, exchangeAName: 'MEXC-SPOT', exchangeBName: 'MEXC-FUTURE' },
        // { pairs: pairsMexcSGateFChunks[i], exchangeA: mexc, exchangeB: gate, exchangeAName: 'MEXC-SPOT', exchangeBName: 'GATE-FUTURE' }
      ]);
    }

    const fetchStartTime = Date.now();
    await Promise.all(
      proxies.map(async (proxy, index) => {
        for (const { pairs, exchangeA, exchangeB, exchangeAName, exchangeBName } of pairsToProcess[index]) {
          const batches = chunkify(pairs, BATCH_SIZE);
          for (const batch of batches) {
            let orderBooksA = [];
            let orderBooksB = [];
      
            const fetchStartTime = Date.now();
            await Promise.all([
              orderBooksA = filterActivePairs(await fetchOrderBooks(exchangeA, batch, exchangeAName, exchangeBName, proxy)),
              orderBooksB = filterActivePairs(await fetchOrderBooks(exchangeB, batch, exchangeBName, exchangeAName, proxy))
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
                  `- Gate: https://www.gate.io/futures/USDT/${pair.replace('/', '_').replace(':USDT', '')}\n` +
                  `- MEXC: https://futures.mexc.com/exchange/${pair.replace('/', '_').replace(':USDT', '')}\n` +
                  `------------------------------------------`
                ;
                await bot.sendMessage(TELEGRAM_CHAT_ID, message, {disable_web_page_preview: true});
                updateOpportunityBuffer(pair, spread);
              }
            }
          }
        }
      })
    )
    console.log(`Total calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
  } catch (err) {
    console.error("An error occurred:", err.message);
  }
};

(async function main() {
  console.log("Starting the bot...");
  
  while (true) {
    await executeArbitrageCheck();
    await new Promise((resolve) => setTimeout(resolve, 1000));

    process.on('SIGINT', () => {
      console.log("\nStopping the bot...");
      process.exit(0);
    });
  }
})();