from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime
import logging
from multiprocessing import Pipe, Pool, Process, cpu_count
from multiprocessing.connection import Connection

from utils import CITIES, YandexWeatherAPI

logging.basicConfig()


class DataFetchingTask:

    data = {}

    def fetch_data(self, city: str):
        """Получает данные с помощью YandexWeatherAPI
        для отдельно взятого города"""
        try:
            ywapi = YandexWeatherAPI()
            resp = ywapi.get_forecasting(city)
            self.data[city] = resp['forecasts']
        except NameError:
            logging.critical('incorrect dictionary key')

    def collect_data(self) -> None:
        """Собирает данные по всем городам"""
        with ThreadPoolExecutor() as executor:
            for city in CITIES:
                executor.submit(self.fetch_data, city=city)

    def get_data(self) -> dict[str, list]:
        """Возвращает собранные данные"""
        return self.data


class DataCalculationTask:

    raw_data = {}
    counted_data = {}
    not_rain_conditions = ('clear', 'partly-cloudy', 'cloudy', 'overcast',)

    def count_av_temp(self, cities: list) -> None:
        """Подсчитывает среднюю температуру, записывает результат в словарь,
        где ключ – название города, значение – словарь,
        который ссодержит температуру и количество часов"""
        city_data = {}
        try:
            for city in cities:
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
                    city_data[forecast['date']] = {'temp': temp,
                                                   'hours': hours}
                self.counted_data[city] = city_data
        except KeyError:
            msg = f'no such city: {city}'
            logging.error(msg)
        return self.counted_data

    def collect_data(self) -> None:
        """Собирает данные по средней температуре и часам без осадков"""
        dft = DataFetchingTask()
        dft.collect_data()
        self.raw_data = dft.get_data()
        cpun = cpu_count() - 1
        with Pool(cpun) as pool:
            self.counted_data = pool.apply(self.count_av_temp, args=(CITIES,))

    def get_aggregated_data(self) -> dict[str, dict[str, float]]:
        """Возвращает собранные данные"""
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
        """парсит дату в нужном формате"""
        date_parsed = datetime.strptime(date_to_parse, "%Y-%m-%d")
        date = date_parsed.strftime("%d-%m")
        return date

    def set_data(self) -> None:
        dct = DataCalculationTask()
        dct.collect_data()
        self.data = dct.get_aggregated_data()

    def get_average_data(self) -> dict[str, tuple[float, float]]:
        return self.average_data

    def aggregate_data_for_city(self, cities: list) -> None:
        av_temp = 0
        av_hours = 0
        try:
            for city in cities:
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
        return self.average_data

    def collect_data(self) -> None:
        self.set_data()
        cpun = cpu_count() - 1
        with Pool(cpun) as pool:
            self.average_data = pool.apply(self.aggregate_data_for_city,
                                           args=(CITIES,))

    def sort_cities(self) -> dict[str, tuple[float, float]]:
        """Сортирует города в порядке убывания предпочтительности
        по температуре и часам"""
        sorted_cities_dict = dict(
            sorted(self.average_data.items(),
                   key=lambda item: (item[1][0], item[1][1]),
                   reverse=True))
        return sorted_cities_dict

    def rate_cities(self, sorted_cities: list, conn: Connection) -> None:
        """Назначает рейтинги городам"""
        for ind, el in enumerate(sorted_cities):
            conn.send((el, ind + 1))
        conn.send(None)

    def write_to_csv(self, conn: Connection) -> None:
        """Запись в файл"""
        field_names = ['Страна/день', '', '26-05', '27-05',
                       '28-05', '29-05', '30-05', 'Среднее', 'Рейтинг']
        with open('data.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(field_names)
            while True:
                data = conn.recv()
                if data is None:
                    break
                city, rate = data
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
                    row.append(rate)
                except (KeyError, IndexError):
                    logging.error('check indexes and keys in lines 157-159')
                row2.append('')
                writer.writerow(row)
                writer.writerow(row2)


class DataAnalyzingTask:

    sorted_cities = {}

    def collect_data(self) -> None:
        """Собирает данные для выбора лучшего города и записи в файл"""
        dat = DataAggregationTask()
        dat.set_data()
        dat.collect_data()
        self.sorted_cities = dat.sort_cities()

    def get_sorted_cities(self) -> dict[str, tuple]:
        """Возвращает словарь с отсортированными городами"""
        return self.sorted_cities

    def choose_best(self) -> str:
        """Выбирает лучший для поездки город"""
        best_cities = []
        dct = self.get_sorted_cities()
        try:
            first = list(dct.keys())[0]
            for key, value in dct.items():
                if value[0] == dct[first][0]:
                    best_cities.append(key)
            best_cities = ' '.join(best_cities)
        except IndexError:
            logging.error('failed aggregated data in DataAnalyzingTask')
        return best_cities

    def save_to_csv(self) -> None:
        dat = DataAggregationTask()
        dat.set_data()
        dat.collect_data()
        conn_sender, conn_reciever = Pipe()
        sender = Process(target=dat.rate_cities,
                         args=(self.sorted_cities, conn_sender,))
        sender.start()
        reciever = Process(target=dat.write_to_csv, args=(conn_reciever,))
        reciever.start()
        sender.join()
        reciever.join()
