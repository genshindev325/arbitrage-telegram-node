const fs = require('fs');
const readline = require('readline');
const ccxt = require('ccxt');
const TelegramBot = require('node-telegram-bot-api');
const _ = require('lodash');

// Divide pairs into smaller chunks to avoid rate-limiting
BATCH_SIZE = 10

// Convert rate limit to seconds, important
EXCHANGE_RATE = 40000.0

// Add delay between batches, important
GATE_RATE = 40000.0

// Max thread counts
MAX_WORKERS = 10

// Require symbol counts
SYMBOL_COUNTS = 2000

// Telegram has a message length limit of 4096 characters.
MAX_ALERTS_PER_MESSAGE = 10

// File to save configuration
const CONFIG_FILE = './TelegramBot_Config.json';

// Helper function to chunk arrays
function chunkify(array, size) {
  return _.chunk(array, size);
}

// Function to prompt for user input
const promptInput = (query) => {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout,
    });

    return new Promise((resolve) => rl.question(query, (answer) => {
        rl.close();
        resolve(answer.trim());
    }));
};

// Function to load or prompt for configuration
async function loadConfig() {
    if (fs.existsSync(CONFIG_FILE)) {
        const config = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf-8'));
        console.log("Configuration loaded successfully.");
        return config;
    } else {
        console.log("Configuration file not found. Please enter the following details:");
        const TELEGRAM_API_KEY = await promptInput('Input TELEGRAM_API_KEY: ');
        const MIN_SPREAD = await promptInput('Input Minimum Percentage for Spread: ');
        const MIN_VOLUME = await promptInput('Input Minimum Volume: ');
        const TELEGRAM_CHAT_ID = await promptInput('Input TELEGRAM_CHAT_ID: ');

        const config = { TELEGRAM_API_KEY, MIN_SPREAD, MIN_VOLUME, TELEGRAM_CHAT_ID };

        // Save the configuration for future runs
        fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 4), 'utf-8');
        console.log("Configuration saved to TelegramBot_Config.json.");
        return config;
    }
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
(async function main() {
  const config = await loadConfig();
  const { TELEGRAM_API_KEY, MIN_SPREAD, MIN_VOLUME, TELEGRAM_CHAT_ID } = config;

  // Initialize Telegram bot
  const bot = new TelegramBot(TELEGRAM_API_KEY, { polling: false });

  console.log("Starting the bot...");
  const scriptStartTime = Date.now();

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
    const commonPairs = _.intersection(gateSymbols, mexcSymbols).slice(0, SYMBOL_COUNTS);
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
      const noOpportunitiesMessage = "🔥 No arbitrage opportunities found!";
      await bot.sendMessage(TELEGRAM_CHAT_ID, noOpportunitiesMessage);
      console.log(noOpportunitiesMessage);
    }
    console.log(`Alerts sent in ${(Date.now() - sendStartTime) / 1000}s`);
  } catch (err) {
    console.error("An error occurred:", err.message);
  }

  console.log(`Script completed in ${(Date.now() - scriptStartTime) / 1000}s`);
  process.exit(0);
})();
