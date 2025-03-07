export const TELEGRAM_API_KEY = '7527533762:AAFL5xUrpYlSRGVBUg3rypxmDFJfr7ErU3g';
export const TELEGRAM_CHAT_ID = '7273835466';
export const MIN_SPREAD = 1; // percent
export const MIN_VOLUME = 100;
export const BUFFER_EXPIRY_TIME = 5 * 60 * 1000; // minutes
export const ARBITRAGE_PERCENTAGE_THRESHOLD = 0.005;
export const MAX_PROCESS_COUNT = 100;
export const MIN_24_VOLUME = 0;
export const proxies = [
  'socks5://4bT5u42E7jZmTuA:lRz2sX6OdH1CWAs@89.38.46.88:42618',
  'socks5://IDmJsKwm8hvoEw9:6M4InkYDdUxKKqf@194.135.30.221:48947',
  'socks5://3JppUgfex0UkELn:71tJ9bki7VUdGju@45.86.94.163:43179',
  'socks5://0d9VgKf7bdxO3K3:ThR9d2u5o5sCQAH@194.135.30.136:43680',
  'socks5://7Li5ZPAXTjQ70Ev:pslMO9pFtMEQP8q@46.151.225.185:41449',
  'socks5://aRz7TohvaO81eJp:BXXc9oeET5CiHjX@212.192.254.151:41205',
  'socks5://qgrRvFfl5eEr8RJ:GBJOq84r6LLaX8H@212.192.254.209:49894',
  'socks5://63aJofI43CUUXXK:VxCBAcc66ozzpjC@45.85.14.77:49552',
  'socks5://uhmu8ks8GQy1Rsi:4UKeNcHVxTxpp1j@45.86.94.145:46328',
  'socks5://mou7U3Gp1mox4DG:5pyDVOcV5YVd4kL@92.114.93.42:47619'
];
export const EXCHANGE_URLS = {
  'Gate-SPOT': 'https://www.gate.io/trade/AAA_USDT', // BTC_USDT
  'Gate-FUTURE': 'https://www.gate.io/futures/USDT/AAA_USDT', // BTC_USDT
  'Mexc-SPOT': 'https://mexc.com/exchange/AAA_USDT', // BTC_USDT
  'Mexc-FUTURE': 'https://futures.mexc.com/exchange/AAA_USDT', // BTC_USDT
  'Bybit-SPOT': 'https://www.bybit.com/en/trade/spot/AAA/USDT', // BTC/USDT
  'Bybit-FUTURE': 'https://www.bybit.com/trade/usdt/AAAUSDT', // GMTUSDT
  'Bitget-SPOT': 'https://www.bitget.com/spot/AAAUSDT', // BTCUSDT
  'Bitget-FUTURE': 'https://www.bitget.com/futures/usdt/AAAUSDT', // BTCUSDT
  'Okx-SPOT': 'https://www.okx.com/trade-spot/AAA-USDT', // eth-usdt
  'Okx-FUTURE': 'https://www.okx.com/trade-swap/AAA-USDT-swap', // xrp-usdt-swap
  'Kucoin-SPOT': 'https://www.kucoin.com/trade/AAA-USDT', // trx-usdt
  'Kucoin-FUTURE': 'https://www.kucoin.com/trade/futures/AAAUSDTM', // trx-usdt
};
export const EXCHANGES = [
  'Gate',
  'Mexc',
  'Bybit',
  'Bitget',
  'Okx',
]