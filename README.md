# CSV Processor

Скрипт для обработки CSV-файлов с поддержкой фильтрации и агрегации данных.

## Возможности

- **Фильтрация**: поддержка операторов `=`, `>`, `<` для любых колонок
- **Агрегация**: расчет `avg`, `min`, `max` для числовых колонок
- **Красивый вывод**: форматированные таблицы в консоли
- **Расширяемая архитектура**: легко добавлять новые операторы и функции

## Установка

```bash
# Установка зависимостей
pip install -r requirements.txt

### Основные команды

```bash
# Фильтрация данных
python csv_processor.py sample_data.csv --filter "brand=xiaomi"
python csv_processor.py sample_data.csv --filter "price>500"
python csv_processor.py sample_data.csv --filter "rating<4.5"

# Агрегация данных
python csv_processor.py sample_data.csv --aggregate "avg=price"
python csv_processor.py sample_data.csv --aggregate "min=rating"
python csv_processor.py sample_data.csv --aggregate "max=price"

# Комбинированное использование
python csv_processor.py sample_data.csv --filter "brand=xiaomi" --aggregate "avg=price"
```

### Примеры

```bash
# Найти все телефоны дороже 500
python csv_processor.py sample_data.csv --filter "price>500"

# Средняя цена всех телефонов
python csv_processor.py sample_data.csv --aggregate "avg=price"

# Средняя цена телефонов Xiaomi
python csv_processor.py sample_data.csv --filter "brand=xiaomi" --aggregate "avg=price"
```

## Тестирование

```bash
# Запуск всех тестов
python -m pytest test_csv_processor.py -v

# Запуск с покрытием кода
python -m pytest test_csv_processor.py --cov=csv_processor --cov-report=html
```

## Архитектура

Проект использует паттерны Strategy и Template Method для обеспечения расширяемости:

- `FilterOperator` - абстрактный базовый класс для операторов фильтрации
- `AggregationFunction` - абстрактный базовый класс для функций агрегации
- `CSVProcessor` - основной класс обработчика



## Структура проекта

```
├── csv_processor.py      # Основной скрипт
├── test_csv_processor.py # Тесты
├── sample_data.csv       # Пример данных
└── README.md            # Документация
```

## Требования к файлам

- CSV файлы должны содержать заголовки в первой строке
- Для агрегации колонки должны содержать только числовые значения
- Кодировка файлов: UTF-8

---

## requirements.txt

```
coverage==7.9.1
exceptiongroup==1.3.0
iniconfig==2.1.0
packaging==25.0
pluggy==1.6.0
Pygments==2.19.1
pytest==8.4.0
pytest-cov==6.2.1
tabulate==0.9.0
tomli==2.2.1
typing_extensions==4.14.0
```