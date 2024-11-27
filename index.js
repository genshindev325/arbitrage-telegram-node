const ccxt = require('ccxt');
const TelegramBot = require('node-telegram-bot-api');
const _ = require('lodash');
const { TELEGRAM_API_KEY, TELEGRAM_CHAT_ID, MIN_SPREAD, MIN_VOLUME } = require('./config');

// Divide pairs into smaller chunks to avoid rate-limiting
BATCH_SIZE = 10

// Convert rate limit to seconds, important
EXCHANGE_RATE = 40000.0

// Add delay between batches, important
GATE_RATE = 40000.0

// Max thread counts
MAX_WORKERS = 10

// Telegram has a message length limit of 4096 characters.
MAX_ALERTS_PER_MESSAGE = 10

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
function calculateSpread(orderBooksA, orderBooksB) {
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

      spreads.push({ pair, direction: 'A->B', spread: spreadAB, buyPrice: asksA[0], sellPrice: bidsB[0] });
      spreads.push({ pair, direction: 'B->A', spread: spreadBA, buyPrice: asksB[0], sellPrice: bidsA[0] });
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
}

// Main function
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

    const alerts = [];
    const batches = chunkify(commonPairs, BATCH_SIZE);

    // Fetch order books in batches
    const fetchStartTime = Date.now();
    for (const batch of batches) {
      const orderBooksGate = filterActivePairs(await fetchOrderBooks(gate, batch));
      const orderBooksMexc = filterActivePairs(await fetchOrderBooks(mexc, batch));

      // Calculate spreads
      const spreads = calculateSpread(orderBooksGate, orderBooksMexc);

      // Collect opportunities
      for (const { pair, spread, direction, buyPrice, sellPrice } of spreads) {
        if (spread >= MIN_SPREAD) {
          const maxVolume = Math.min(
            orderBooksGate[pair]?.asks[0][1] || 0,
            orderBooksMexc[pair]?.bids[0][1] || 0
          );

          if (maxVolume >= MIN_VOLUME) {
            const profit = (spread * maxVolume * buyPrice) / 100;
            console.log(`Alert added for ${pair}: Spread: ${spread}%, Profit: ${profit}`);
            alerts.push(
              `Coin: ${pair}\n` +
              `Direction: ${direction}\n` +
              `Spread: ${spread.toFixed(2)}%\n` +
              `Max Volume: ${maxVolume.toFixed(2)}\n` +
              `Potential Profit: ${profit.toFixed(2)} USDT\n` +
              `Links:\n` +
              `- Gate: https://www.gate.io/futures/USDT/${pair.replace('/', '_').replace(':USDT', '')}\n` +
              `- MEXC: https://www.mexc.com/futures/USDT/${pair.replace('/', '_').replace(':USDT', '')}\n` +
              `------------------------------------------`
            );
          }
        }
      }
    }
    console.log(`Order books fetched and spreads calculated in ${(Date.now() - fetchStartTime) / 1000}s`);

    // Send alerts in batches
    const sendStartTime = Date.now();
    if (alerts.length) {
      const alertChunks = chunkify(alerts, MAX_ALERTS_PER_MESSAGE);
      for (const chunk of alertChunks) {
        const message = `🔥 Arbitrage Opportunities Found!\n\n${chunk.join('\n\n')}`;
        await bot.sendMessage(TELEGRAM_CHAT_ID, message);
      }
      console.log("Summary alert sent.");
    } else {
      console.log("No arbitrage opportunities found!");
    }
    console.log(`Alerts sent in ${(Date.now() - sendStartTime) / 1000}s`);
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