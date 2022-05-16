# ceres
a piece of uranus, part of market data collection/relay and cross exchange trading system.

The production ready binance spot/usdt-margin/coin-margin wss market crawler included.
Full order functional Gemini market client included.

## Dependency
Make sure https://github.com/ziwei-yang/aphrodite in the same parent directory.
```
Proj/aphrodite
Proj/ceres
```

## Preparation:
```
For market clients:
rvm install 3.0.0
bundle install

For market crawlers:
npm install -d
```

## Backtesting Data:
```
Proj/ceres/data/
└── subscribe
    ├── Bybit_USD-BTC@P.20191110_165906.txt.gz
    ├── Bybit_USD-BTC@P.20191125_123626.txt.gz
```

## Run dummy backtesing:
./exec.sh trader/dmy.rb TEST_FILE_FILTER
