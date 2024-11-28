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
const BATCH_SIZE = Math.min(Math.max(proxies.length, 10), MAX_PROCESS_COUNT);
process.setMaxListeners(MAX_PROCESS_COUNT);

function chunkify(array, size) {
  return _.chunk(array, size);
}

async function fetchOrderBooks(exchange, pairs, proxyAgent) {
  const orderBooks = {};
  await Promise.all(
    pairs.map(async (pair) => {
      try {
        const orderBook = await exchange.fetchOrderBook(pair, { 'proxy': proxyAgent });
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
    const mexcSpotSymbols = mexcSymbols.filter((symbol) => symbol.endsWith('/USDT') && !symbol.includes(':'));
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
      return mexcFutureSymbols.includes(futuresSymbol);
    })
    const PairsGateFMexcF = _.intersection(gateFutureSymbols, mexcFutureSymbols);
    const batches = chunkify(PairsGateFMexcF, BATCH_SIZE);

    console.log(`Markets loaded in ${(Date.now() - marketStartTime) / 1000}s`);
    console.log(`Number of common trading pairs (Gate-spot / Gate-futures): ${PairsGateSGateF.length}`);
    console.log(`Number of common trading pairs (Gate-spot / Mexc-futures): ${PairsGateSMexcF.length}`);
    console.log(`Number of common trading pairs (Mexc-spot / Mexc-futures): ${PairsMexcSMexcF.length}`);
    console.log(`Number of common trading pairs (Mexc-spot / Gate-futures): ${PairsMexcSGateF.length}`);
    console.log(`Number of common trading pairs (Gate-futures / Mexc-futures): ${PairsGateFMexcF.length}`);

    const fetchStartTime = Date.now();
    for (const batch of batches) {
      let orderBooksGate = [];
      let orderBooksMexc = [];

      await Promise.all([
        orderBooksGate = filterActivePairs(await fetchOrderBooks(gate, batch, proxies[0])),
        orderBooksMexc = filterActivePairs(await fetchOrderBooks(mexc, batch, proxies[1]))
      ])
  
      const profits = calculateProfit(orderBooksGate, orderBooksMexc, 'GATE', 'MEXC');
  
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
    console.log(`Order books fetched and spreads calculated in ${(Date.now() - fetchStartTime) / 1000}s`);
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