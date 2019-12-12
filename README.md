# ceres
a piece of uranus

## Dependency
Make sure https://github.com/ziwei-yang/aphrodite in the same parent directory.
Proj/aphrodite
Proj/ceres

## Preparation:
rvm install 2.6
bundle install

## Data:
```
Proj/ceres/data/
└── subscribe
    ├── Bybit_USD-BTC@P.20191110_165906.txt.gz
    ├── Bybit_USD-BTC@P.20191125_123626.txt.gz
```

## Run dummy backtesing:
./exec.sh trader/dmy.rb TEST_FILE_FILTER
