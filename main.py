from datetime import datetime, timedelta
from pandas import DataFrame
from tinkoff.invest import Client, CandleInterval, HistoricCandle, RequestError
from tinkoff.invest.async_services import InstrumentsService, MarketDataService
import pandas as pd




class StockParser:
    def __init__(self):
        self.token = None  # Initialize token

    @staticmethod
    def create_df(candles: [HistoricCandle]):
        df = DataFrame([{
            'date': c.time.date(),
            'volume': c.volume,
            'open': StockParser.cast_money(c.open),
            'close': StockParser.cast_money(c.close),
            'high': StockParser.cast_money(c.high),
            'low': StockParser.cast_money(c.low),
        } for c in candles])

        return df

    @staticmethod
    def cast_money(v):
        return v.units + v.nano / 1e9  # nano - 9 нулей

    @staticmethod
    def get_candles(figi, start_date, end_date, token):

        try:
            with Client(token) as client:
                r = client.market_data.get_candles(
                    figi=figi,
                    from_=start_date,
                    to=end_date,
                    interval=CandleInterval.CANDLE_INTERVAL_DAY
                )
                return r
        except RequestError as e:
            print(str(e))

    @staticmethod
    def split_date_range(start_date, end_date):
        date_ranges = []
        current_start = start_date
        current_end = start_date + timedelta(days=350)

        while current_start < end_date:
            if current_end > end_date:
                current_end = end_date

            date_ranges.append((current_start, current_end))

            current_start = current_end + timedelta(days=1)
            current_end = current_start + timedelta(days=350)

        return date_ranges

    def run(self, tickers, start_date=None, end_date=None):
        token = input("Enter your token: ")  # Get token as input
        with Client(token) as cl:
            instruments: InstrumentsService = cl.instruments
            market_data: MarketDataService = cl.market_data

            l = []
            for method in ['shares', 'bonds']:
                for item in getattr(instruments, method)().instruments:
                    l.append({
                        'ticker': item.ticker,
                        'figi': item.figi,
                        'type': method,
                        'name': item.name,
                    })

            df = DataFrame(l)
            df = df[df['ticker'].isin(tickers)]
            if df.empty:
                print(f"Нет тикеров из списка: {', '.join(tickers)}")
                return

            merged_df = DataFrame()

            for ticker in tickers:
                asset_data = df[df['ticker'] == ticker]
                if asset_data.empty:
                    print(f"Тикер не найден: {ticker}")
                    continue

                figi = asset_data['figi'].values[0]
                asset_name = asset_data['name'].values[0]

                try:
                    if start_date and end_date:
                        date_ranges = self.split_date_range(start_date, end_date)
                    else:
                        date_ranges = [(start_date, end_date)]

                    for range_start, range_end in date_ranges:
                        r = self.get_candles(figi, range_start, range_end, token)
                        if r:
                            temp_df = self.create_df(r.candles)
                            temp_df.rename(columns={'close': asset_name}, inplace=True)
                            temp_df = temp_df[['date', asset_name]]

                            if merged_df.empty:
                                merged_df = temp_df
                            else:
                                merged_df = merged_df.merge(temp_df, on='date', how='outer')
                except:
                    print(f"Ошибка при получении данных для тикера: {asset_name}")
                    print()

            columns = merged_df.columns
            unique_columns = set([col.rsplit('_', 1)[0] for col in columns])

            for column_name in unique_columns:
                relevant_columns = [col for col in columns if col.startswith(column_name)]
                combined_column = relevant_columns[-1].rsplit('_', 1)[0]
                merged_df[combined_column] = merged_df[relevant_columns].bfill(axis=1).iloc[:, 0]
                merged_df = merged_df.drop(relevant_columns[:-1], axis=1)

            with pd.option_context('display.max_columns', None, 'display.max_rows', None):
                print(merged_df.info())
            print(token)
            return merged_df

    def main(self):
        TICKER = input("Enter tickers separated by commas: ").split(",")
        TICKERS = ['MOEX']
        TICKERS.extend(TICKER)
        START_DATE = input("Enter start date (yyyy-mm-dd): ")
        END_DATE = input("Enter end date (yyyy-mm-dd): ")

        if START_DATE and END_DATE:
            start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
            end_date = datetime.strptime(END_DATE, "%Y-%m-%d")
            df = self.run(TICKERS, start_date, end_date)
        else:
            df = self.run(TICKERS)

        print(df)


if __name__ == "__main__":
    analyzer = StockParser()
    analyzer.main()
