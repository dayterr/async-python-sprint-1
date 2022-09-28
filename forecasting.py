from datetime import datetime

from tasks import DataAnalyzingTask


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    start = datetime.now()
    dant = DataAnalyzingTask()
    dant.collect_data()
    dant.save_to_csv()
    print(dant.choose_best())
    print(datetime.now() - start)


if __name__ == "__main__":
    forecast_weather()
