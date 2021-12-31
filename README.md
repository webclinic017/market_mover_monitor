# market_mover_monitor
PROJECT SETUP:

1. Run py -m venv VENV_NAME
2. Run pip install -r requirements.txt


HOW TO USE:

1. Download Interactive Brokers Trader Workstation (TWS) or IB Gateway
2. Set Interactive Brokers timezone to US/Eastern
3. Login Interactive Brokers account through TWS or IB Gateway (Interactive Brokers trading account with market data subscription is necessary for the program)
4. Run market_mover_monitor.py while TWS or IB Gateway is running


FUNCTION:

1. Once program starts it keep scanning top gainers in pre-market or normal trading hours, depending on program start time. 1 minute and 5 minute candle will be retrieved for pattern analysis, include new top gainer scanning, high volume ramp up etc, patterns/ strategys are still optimising/ developing 