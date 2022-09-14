import csv
from datetime import datetime
import logging
import multiprocessing
import threading

from utils import CITIES, YandexWeatherAPI

logging.basicConfig()


class DataFetchingTask:

    data = {}

    def fetch_data(self, city: str):
        try:
            ywapi = YandexWeatherAPI()
            resp = ywapi.get_forecasting(city)
            self.data[city] = resp['forecasts']
        except Exception:
            logging.critical('something wrong')

    def collect_data(self) -> None:
        for city in CITIES:
            x = threading.Thread(target=self.fetch_data,
                                 args=(city,), name="thread_1")
            x.start()
            x.join()

    def get_data(self) -> None:
        return self.data


class DataCalculationTask:

    raw_data = {}
    counted_data = {}
    not_rain_conditions = ('clear', 'partly-cloudy', 'cloudy', 'overcast',)

    def count_av_temp(self, city: str) -> None:
        city_data = {}
        try:
            for forecast in self.raw_data[city]:
                hours = 0
                temp = 0
                try:
                    for hour in forecast['hours'][9:20]:
                        temp += hour['temp']
                        if hour['condition'] in self.not_rain_conditions:
                            hours += 1
                except KeyError:
                    logging.error('check the keys is lines 46-48')
                temp /= 10
                temp = round(temp, 1)
                city_data[forecast['date']] = {'temp': temp, 'hours': hours}
            self.counted_data[city] = city_data
        except KeyError:
            msg = f'no such city: {city}'
            logging.error(msg)

    def collect_data(self) -> None:
        dft = DataFetchingTask()
        dft.collect_data()
        self.raw_data = dft.get_data()
        for city in CITIES:
            x = threading.Thread(target=self.count_av_temp,
                                 args=(city,), name="thread_1")
            x.start()
            x.join()

    def get_aggregated_data(self) -> dict[str, dict[str, float]]:
        return self.counted_data


class DataAggregationTask:

    data = {}
    average_data = {}
    dates = []
    cities = {'MOSCOW': 'Москва', 'PARIS': 'Париж', 'LONDON': 'Лондон',
              'BERLIN': 'Берлин', 'BEIJING': 'Пекин',
              'KAZAN': 'Казань', 'SPETERSBURG': 'Санкт-Петербург',
              'VOLGOGRAD': 'Волгоград',
              'NOVOSIBIRSK': 'Новосибирск', 'KALININGRAD': 'Калининград',
              'ABUDHABI': 'Абу-Даби',
              'WARSZAWA': 'Варшава', 'BUCHAREST': 'Бухарест',
              'ROMA': 'Рим', 'CAIRO': 'Каир'}
    rates = {}

    def parse_date(self, date_to_parse: str) -> str:
        date_parsed = datetime.strptime(date_to_parse, "%Y-%m-%d")
        date = date_parsed.strftime("%d-%m")
        return date

    def set_data(self) -> None:
        dct = DataCalculationTask()
        dct.collect_data()
        self.data = dct.get_aggregated_data()

    def get_data(self) -> dict[str, dict[str, float]]:
        return self.data

    def get_average_data(self) -> dict[str, tuple[float, float]]:
        return self.average_data

    def aggregate_data_for_city(self, city) -> None:
        av_temp = 0
        av_hours = 0
        try:
            for day, data in self.data[city].items():
                if len(self.dates) < 5:
                    date = self.parse_date(day)
                    self.dates.append(date)
                av_temp += data['temp']
                av_hours += data['hours']
            av_temp /= 5
            av_hours /= 5
            av_temp = round(av_temp, 1)
            self.average_data[city] = av_temp, av_hours
        except KeyError:
            logging.error('check the keys in lines 105-114')

    def collect_data(self) -> None:
        for city in CITIES:
            self.aggregate_data_for_city(city)
            # x = multiprocessing.Process(target=self.aggregate_data_for_city,
            # args=(city,))
            # x.start()
            # x.join()

    def sort_cities(self) -> dict[str, tuple[float, float]]:
        dct = dict(sorted(self.average_data.items(),
                          key=lambda item: (item[1][0], item[1][1]),
                          reverse=True))
        return dct

    def rate_cities(self) -> None:
        dct = self.sort_cities().keys()
        lst = list(dct)
        for ind, el in enumerate(lst):
            gorod = self.cities[el]
            self.rates[gorod] = ind + 1

    def write_to_csv(self) -> None:
        field_names = ['Страна/день', '', '26-05', '27-05',
                       '28-05', '29-05', '30-05', 'Среднее', 'Рейтинг']
        with open('data.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(field_names)
            for city in CITIES:
                row = []
                row2 = []
                gorod = self.cities[city]
                row.append(gorod)
                row2.append('')
                row.append('Температура, среднее')
                row2.append('Без осадков, часов')
                try:
                    for _, day in self.data[city].items():
                        row.append(day['temp'])
                        row2.append(day['hours'])
                except KeyError:
                    msg = f'can\'t find such city: {city}'
                    logging.error(msg)
                try:
                    row.append(self.average_data[city][0])
                    row2.append(self.average_data[city][1])
                    row.append(self.rates[gorod])
                except (KeyError, IndexError):
                    logging.error('check indexes and keys in lines 157-159')
                row2.append('')
                writer.writerow(row)
                writer.writerow(row2)

    def get_rates(self) -> dict[str, int]:
        return self.rates


class DataAnalyzingTask:

    aggregated_data = {}

    def collect_data(self) -> None:
        dat = DataAggregationTask()
        dat.set_data()
        dat.collect_data()
        dat.rate_cities()
        dat.write_to_csv()
        self.aggregated_data = dat.sort_cities()

    def get_aggregated_data(self) -> dict[str, tuple[float, float]]:
        return self.aggregated_data

    def choose_best(self) -> str:
        best_cities = []
        dct = self.get_aggregated_data()
        first = list(dct.keys())[0]
        for key, value in dct.items():
            if value[0] == dct[first][0]:
                best_cities.append(key)
        best_cities = ' '.join(best_cities)
        return best_cities


dant = DataAnalyzingTask()
dant.collect_data()
print(dant.choose_best())
