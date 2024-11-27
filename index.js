import ccxt from 'ccxt';
import TelegramBot from 'node-telegram-bot-api';
import _ from 'lodash';
import { TELEGRAM_API_KEY, TELEGRAM_CHAT_ID, MIN_SPREAD, MIN_VOLUME, BUFFER_EXPIRY_TIME, ARBITRAGE_PERCENTAGE_THRESHOLD } from "./config.js";

const BATCH_SIZE = 10;
const EXCHANGE_RATE = 40000.0;
const opportunityBuffer = {};

// Helper function to chunk arrays
function chunkify(array, size) {
  return _.chunk(array, size);
}

// Fetch order books concurrently
async function fetchOrderBooks(exchange, pairs) {
  const orderBooks = {};
  const delay = exchange.rateLimit / EXCHANGE_RATE;

  await Promise.all(
    pairs.map(async (pair) => {
      try {
        const orderBook = await exchange.fetchOrderBook(pair);
        orderBooks[pair] = orderBook;
        await new Promise((resolve) => setTimeout(resolve, delay));
      } catch (err) {
        console.error(`Error fetching order book for ${pair}:`, err.message);
      }
    })
  );

  return orderBooks;
}

// Calculate spreads
function calculateSpread(orderBooksA, orderBooksB, exchangeAName, exchangeBName) {
  const spreads = [];

  for (const pair in orderBooksA) {
    if (orderBooksB[pair]) {
      const asksA = orderBooksA[pair].asks[0];
      const bidsB = orderBooksB[pair].bids[0];
      const bidsA = orderBooksA[pair].bids[0];
      const asksB = orderBooksB[pair].asks[0];

      if (!asksA || !bidsB || !bidsA || !asksB) continue;

      const spreadAB = ((bidsB[0] - asksA[0]) / asksA[0]) * 100;
      const spreadBA = ((bidsA[0] - asksB[0]) / asksB[0]) * 100;

      spreads.push({ pair, direction: `${exchangeAName}->${exchangeBName}`, spread: spreadAB, buyPrice: asksA[0], sellPrice: bidsB[0] });
      spreads.push({ pair, direction: `${exchangeBName}->${exchangeAName}`, spread: spreadBA, buyPrice: asksB[0], sellPrice: bidsA[0] });
    }
  }

  return spreads;
}

function filterActivePairs(orderBooks) {
  return Object.keys(orderBooks).reduce((filtered, pair) => {
    if (orderBooks[pair]?.asks?.length && orderBooks[pair]?.bids?.length) {
      filtered[pair] = orderBooks[pair];
    }
    return filtered;
  }, {});
}// Check if an opportunity should be notified
function shouldNotifyOpportunity(pair, newPercentage) {
  const now = Date.now();

  if (!opportunityBuffer[pair]) {
    // If the pair is not in the buffer, notify
    return true;
  }

  const { lastPercentage, lastTimestamp } = opportunityBuffer[pair];

  const timeElapsed = now - lastTimestamp;
  const percentageChange = newPercentage - lastPercentage;

  // Notify if time interval has passed or percentage threshold is exceeded
  return (
    timeElapsed > BUFFER_EXPIRY_TIME ||
    percentageChange >= ARBITRAGE_PERCENTAGE_THRESHOLD
  );
}

// Update the buffer with the latest data
function updateOpportunityBuffer(pair, percentage) {
  opportunityBuffer[pair] = {
    lastPercentage: percentage,
    lastTimestamp: Date.now(),
  };
}

async function executeArbitrageCheck() {
  // Initialize Telegram bot
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });

  // Initialize exchanges
  const gate = new ccxt.gateio();
  const mexc = new ccxt.mexc();

  try {
    // Load markets
    await gate.loadMarkets({ options: { defaultType: 'future' } });
    await mexc.loadMarkets({ options: { defaultType: 'future' } });

    // Filter futures pairs
    console.log("Loading markets...");
    const marketStartTime = Date.now();
    const gateSymbols = Object.keys(gate.markets).filter((symbol) => symbol.includes('/USDT:'));
    const mexcSymbols = Object.keys(mexc.markets).filter((symbol) => symbol.includes('/USDT:'));
    const commonPairs = _.intersection(gateSymbols, mexcSymbols);
    console.log(`Markets loaded in ${(Date.now() - marketStartTime) / 1000}s`);
    console.log(`Number of common trading pairs (futures): ${commonPairs.length}`);

    const batches = chunkify(commonPairs, BATCH_SIZE);

    // Fetch order books in batches
    const fetchStartTime = Date.now();
    for (const batch of batches) {
      const orderBooksGate = filterActivePairs(await fetchOrderBooks(gate, batch));
      const orderBooksMexc = filterActivePairs(await fetchOrderBooks(mexc, batch));

      // Calculate spreads
      const spreads = calculateSpread(orderBooksGate, orderBooksMexc, 'GATE', 'MEXC');

      // Collect opportunities
      for (const { pair, spread, direction, buyPrice, sellPrice } of spreads) {
        if (spread >= MIN_SPREAD) {
          const maxVolume = Math.min(
            orderBooksGate[pair]?.asks[0][1] || 0,
            orderBooksMexc[pair]?.bids[0][1] || 0
          );

          if (maxVolume >= MIN_VOLUME) {
            if (shouldNotifyOpportunity(pair, spread)) {
              const profit = maxVolume * Math.abs(buyPrice - sellPrice);
              console.log(`Alert sent for ${pair}: Spread: ${spread}%, Profit: ${profit}`);
              const message =
                `Coin: ${pair}\n` +
                `Direction: ${direction}\n` +
                `Spread: ${spread.toFixed(2)}%\n` +
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
    }
    console.log(`Order books fetched and spreads calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
  } catch (err) {
    console.error("An error occurred:", err.message);
  }
};

(async function main() {
  console.log("Bot started.");
  
  while (true) {
    await executeArbitrageCheck();
    await new Promise((resolve) => setTimeout(resolve, 1000));

    process.on('SIGINT', () => {
      console.log("\nStopping the bot...");
      process.exit(0);
    });
  }
})();