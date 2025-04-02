# dev memo

Thanks to Deribit history API, we have the following APIs to collect historical data:

``` text
All Historical Data available since Deribit's launch

To facilitate statistical and quantitative analysis, backtesting, price action reviews etc we have made a series of new endpoints available offering access to all "old" trade and instrument information.

The data consists of all trades and instruments since 2016, the launch of Deribit

We currently via our API only provide 5 days of history, this has now been expanded to complete history. The historical Database is updated approximately 5 seconds after the trade has occurred so for real time data please continue using the current endpoints.

The specs are the same as the currently active endpoints, see docs.deribit.com.

New endpoints (history instead of www)
https://history.deribit.com/api/v2/public/get_instrument
https://history.deribit.com/api/v2/public/get_instruments
https://history.deribit.com/api/v2/public/get_last_trades_by_currency
https://history.deribit.com/api/v2/public/get_last_trades_by_currency_and_time
https://history.deribit.com/api/v2/public/get_last_trades_by_instrument
https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time

👉 count = max 10.000 & 'include_old = true' must be enabled
```

In order to fetch historical trade data, we need to use these APIs:

- Fetch all instruments: `https://history.deribit.com/api/v2/public/get_instruments`
- Fetch all trades of an instrument: `https://history.deribit.com/api/v2/public/get_last_trades_by_instrument_and_time`
  - It can only return a maximum of the most recent 10,000 transaction records within the specified time window. To retrieve all the data, we may need to call this API multiple times.
  - For options' trades, that's not a problem, because only few options have more than 10,000 trades.
  - For futures' trades, the time range needs to be considered because a future usually have way more than 10,000 trades, if we sequentially query it would cost huge time.

## fetching options and their trades

1. fetch option list
2. for each option in the list, noted by (instrument_name, creation_ts, expire_ts)
3. query its trade records parallelly
   1. if the result contains 10,000 trades, get the earliest trade's ts as a new endpoint, query (instrument_name, creation_ts, new_end_ts - 1)
   2. if the result contains less than 10,000 trades, it means the option's trades are collected completely.
4. save all trades of an option as .csv file

## fetching futures and their trades

1. fetch future list
2. for each future in the list, noted by (instrument_name, creation_ts, expire_ts)
3. split the (creation_ts, expire_ts) into multiple ranges by day? hour?
4. query each time window's trade records parallelly
5. query a time window's trade records parallelly
   1. if the result contains 10,000 trades, get the earliest trade's ts as a new endpoint, query (instrument_name, creation_ts, new_end_ts - 1)
   2. if the result contains less than 10,000 trades, it means all trades of a time range of a future are collected completely.
6. save all trades of a future's given time range as .csv file

## TODO

### save into database

- clickhouse?
- influxdb?

### order history

In 1 April 2025, [Deribit](https://support.deribit.com/hc/en-us/articles/25973087226909-Accessing-historical-trades-and-orders-using-API) supported new endpoints:

- `private/get_order_history_by_instrument`
- `private/get_order_history_by_currency`
- `private/get_user_trades_by_instrument`
- `private/get_user_trades_by_instrument_and_time`
- `private/get_user_trades_by_currency`
- `private/get_user_trades_by_currency_and_time`
- `private/get_user_trades_by_order`

Maybe we can use it to fetch all historcal orders, not only limit to trade orders.
