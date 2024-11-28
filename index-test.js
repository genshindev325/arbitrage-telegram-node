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

process.setMaxListeners(MAX_PROCESS_COUNT);

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
    // Load markets for Gate.io and MEXC
    await Promise.all([gate.loadMarkets(), mexc.loadMarkets()]);

    const gateSymbols = Object.keys(gate.markets);
    const mexcSymbols = Object.keys(mexc.markets);

    // Filter spot and futures pairs for Gate and MEXC
    const gateSpotSymbols = gateSymbols.filter(symbol => symbol.endsWith('/USDT') && !symbol.includes(':'));
    const gateFutureSymbols = gateSymbols.filter((symbol) => symbol.includes('/USDT:'));
    const mexcSpotSymbols = mexcSymbols.filter((symbol) => symbol.endsWith('/USDT') && !symbol.includes(':'));
    const mexcFutureSymbols = mexcSymbols.filter((symbol) => symbol.includes('/USDT:'));

    // Identify common pairs between spot and futures for Gate and MEXC
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

    console.log("Markets loaded...");

    // Start fetching order books in parallel for the first 5 pairs
    const pairsToProcess = [
      { pairs: PairsGateSGateF, gate: gate, mexc: null, gateName: 'GATE', mexcName: null },
      { pairs: PairsGateSMexcF, gate: gate, mexc: mexc, gateName: 'GATE', mexcName: 'MEXC' },
      { pairs: PairsMexcSMexcF, gate: null, mexc: mexc, gateName: null, mexcName: 'MEXC' },
      { pairs: PairsMexcSGateF, gate: gate, mexc: mexc, gateName: 'GATE', mexcName: 'MEXC' },
      { pairs: PairsGateFMexcF, gate: gate, mexc: mexc, gateName: 'GATE', mexcName: 'MEXC' }
    ];

    const fetchStartTime = Date.now();

    await Promise.all(
      pairsToProcess.map(async ({ pairs, gate, mexc, gateName, mexcName }) => {
        let orderBooksGate = [];
        let orderBooksMexc = [];

        if (gate && pairs.length > 0) {
          orderBooksGate = filterActivePairs(await fetchOrderBooks(gate, pairs, proxies[0]));
        }

        if (mexc && pairs.length > 0) {
          orderBooksMexc = filterActivePairs(await fetchOrderBooks(mexc, pairs, proxies[1]));
        }

        const profits = calculateProfit(orderBooksGate, orderBooksMexc, gateName, mexcName);

        for (const { pair, spread, direction, buyPrice, sellPrice, maxVolume, profit } of profits) {
          if (shouldNotifyOpportunity(pair, spread)) {
            const message = `Coin: ${pair}\nDirection: ${direction}\nSpread: ${spread.toFixed(2)}%\nBuy Price: ${buyPrice}\nSell Price: ${sellPrice}\nMax Volume: ${maxVolume.toFixed(2)}\nPotential Profit: ${profit.toFixed(2)} USDT\nLinks:\n- Gate: https://www.gate.io/futures/USDT/${pair.replace('/', '_').replace(':USDT', '')}\n- Mexc: https://futures.mexc.com/exchange/${pair.replace('/', '_').replace(':USDT', '')}\n------------------------------------------`;
            await bot.sendMessage(TELEGRAM_CHAT_ID, message, { disable_web_page_preview: true });
            updateOpportunityBuffer(pair, spread);
          }
        }
      })
    );
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