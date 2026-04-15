# HomeCyBot

A professional starter bot for pair trading and statistical arbitrage research with Interactive Brokers.

## Current scope

This version implements the first stage only:

- connect to the paper trading gateway
- verify API access
- read account summary and positions
- request market data for multiple symbols
- request a small historical sample

## Planned bot roadmap

1. collect and validate historical data
2. rank candidate pairs with simple statistical filters
3. define entry and exit rules
4. add alerts and monitoring

## Build order

We will implement the bot from easier to harder:

1. pair statistics core
2. candidate pair scanner
3. trading signals
4. monitoring and notifications

## Configuration

All bot settings are stored in JSON.

Use [config/settings.example.json](config/settings.example.json) as the template and place your runtime values in [config/settings.json](config/settings.json).
The bot reads all connection details only from that JSON file.

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_probe.py
```

You can also override the watchlist for a single run:

```bash
python run_probe.py --symbols AAPL MSFT SPY QQQ XOM CVX
```

## Expected output

A successful probe returns:

- connection status
- server time
- account summary
- current positions
- multiple quotes
- historical sample data
- ranked pair candidates when pair scanning is enabled

## Pair scanner

The basic pair scanner can now rank symbols from the configured watchlist using:

- price correlation
- hedge ratio fit
- spread z-score
- mean-crossing stability

## Core files

- [src/homecybot/main.py](src/homecybot/main.py)
- [src/homecybot/config.py](src/homecybot/config.py)
- [src/homecybot/ib_client.py](src/homecybot/ib_client.py)
- [config/settings.example.json](config/settings.example.json)
