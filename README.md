# WebSocketExchangeApp

Тестовое приложение для сбора биржевых тиков из нескольких WebSocket-источников, приведения их к единому виду и записи в PostgreSQL.

## Что делает приложение

Решение состоит из двух запускаемых проектов:

- `FakeExchangeHost` поднимает тестовые WebSocket-источники, которые имитируют несколько бирж.
- `Aggregator.Application` подключается к этим источникам, читает сообщения, приводит данные каждой биржи к общему формату `TradeTick`, группирует записи в пакеты и сохраняет их в PostgreSQL.

Поддерживаемые источники:

- `binance`
- `coinbase`
- `kraken`

## Как устроено решение

- `src/Aggregator.Domain`  
  Здесь лежит основная предметная модель `TradeTick`, с которой дальше работает приложение.

- `src/Aggregator.Core`  
  Здесь лежат общие контракты и вспомогательные вещи, которые не должны зависеть от конкретной биржи, базы данных или способа запуска.

- `src/Aggregator.Application`  
  Здесь лежит основная рабочая логика приложения: подключение к WebSocket-источникам, чтение сообщений, нормализация, переподключение, накопление данных в пакеты, сбор статистики и запуск фонового процесса.

- `src/Aggregator.Infrastructure`  
  Здесь лежит работа с PostgreSQL: контекст базы данных, миграции и запись тиков в таблицу.

- `src/FakeExchangeHost`  
  Здесь лежит тестовый сервер, который отдает фиктивные биржевые данные по WebSocket.

- `tests/UnitTests`  
  Здесь лежат модульные тесты для нормализации, пакетной записи, переподключения и вспомогательной логики.

## Требования

- .NET 8 SDK
- PostgreSQL 14+ или совместимая версия

## Сборка

```bash
dotnet build WebSocketExchangeApp.sln
```

## Запуск

### 1. Подготовить PostgreSQL

Приложение берет строку подключения из файла:

- `src/Aggregator.Application/appsettings.json`

Текущее значение:

```json
"ConnectionStrings": {
  "Postgres": "Host=localhost;Port=5432;Database=postgres;Username=aggregator_app;Password=change_me_strong_password"
}
```

Нужно:

1. запустить PostgreSQL;
2. создать пользователя и пароль, соответствующие конфигу, либо заменить строку подключения на свою.

На старте приложение само применяет миграции. Если подключения к базе нет, `Aggregator.Application` не запустится.

Пример SQL-скрипта для создания пользователя и выдачи прав под текущий конфиг:

```postgresql
CREATE ROLE aggregator_app LOGIN PASSWORD 'change_me_strong_password';

GRANT CONNECT ON DATABASE postgres TO aggregator_app;

GRANT USAGE, CREATE ON SCHEMA public TO aggregator_app;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO aggregator_app;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO aggregator_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO aggregator_app;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO aggregator_app;
```

### 2. Запустить тестовые WebSocket-источники

```bash
dotnet run --project src/FakeExchangeHost/FakeExchangeHost.csproj
```

По текущим настройкам проект запускается на:

- `http://localhost:5121`

Используемые WebSocket-адреса:

- `ws://localhost:5000/ws/binance`
- `ws://localhost:5001/ws/coinbase`
- `ws://localhost:5002/ws/kraken`

Если локально ты менял конфиг, ориентируйся на `src/Aggregator.Application/appsettings.json`.

### 3. Запустить агрегатор

```bash
dotnet run --project src/Aggregator.Application/Aggregator.Application.csproj
```

При запуске приложение:

1. проверяет конфигурацию;
2. применяет миграции базы данных;
3. подключается к WebSocket-источникам;
4. начинает читать сообщения;
5. приводит их к единому виду;
6. сохраняет данные в PostgreSQL.

## Статистика работы

Приложение поднимает HTTP endpoint со статистикой:

- `http://localhost:5180/debug/stats`

Метод:

- `GET`

Если порт `5180` занят, приложение продолжит работать, но endpoint со статистикой не поднимется. Это будет видно в логах.

### Что возвращает endpoint

Endpoint возвращает JSON со снимком текущего состояния приложения.

Основные поля:

- `rawReceived` — сколько исходных сообщений получено всего
- `channelRead` — сколько сообщений прошло через внутренний канал обработки
- `normalizedOk` — сколько сообщений удалось успешно привести к общему формату
- `normalizedFailed` — сколько сообщений не удалось разобрать
- `readPerSecond` — примерная текущая скорость чтения
- `reconnectAttemptsTotal` — общее число попыток переподключения
- `connectFailures` — сколько раз не удалось установить соединение
- `batchesFlushedTotal` — сколько пакетов записано в базу
- `lastBatchSize` — размер последнего записанного пакета
- `connections` — те же показатели по каждому источнику отдельно

Пример структуры ответа:

```json
{
  "rawReceived": 1200,
  "channelRead": 1200,
  "normalizedOk": 1198,
  "normalizedFailed": 2,
  "readPerSecond": 98.4,
  "reconnectAttemptsTotal": 1,
  "connectFailures": 0,
  "batchesFlushedTotal": 12,
  "lastBatchSize": 100,
  "connections": {
    "Binance": {
      "rawReceived": 400,
      "channelRead": 400,
      "normalizedOk": 400,
      "normalizedFailed": 0,
      "readPerSecond": 33.1,
      "reconnectAttemptsTotal": 0,
      "reconnectAttemptsCurrentCycle": 0,
      "connectFailures": 0,
      "lastReconnectDelayMs": 0
    }
  }
}
```

## Конфигурация

Основной конфиг лежит в:

- `src/Aggregator.Application/appsettings.json`

### `ConnectionStrings:Postgres`

Это строка подключения к PostgreSQL.

Что она определяет:

- сможет ли приложение вообще запуститься;
- куда будут применяться миграции;
- в какую базу будут записываться тики.

### `Exchange:Connections`

Это список подключений к WebSocket-источникам.

Пример одного элемента:

```json
{
  "Url": "ws://localhost:5000/ws/binance",
  "Source": "Binance",
  "Reconnect": {
    "MaxAttempts": 0,
    "ConnectTimeoutMs": 5000,
    "DelayMs": 3000,
    "MaxDelayMs": 30000,
    "JitterRatio": 0.2
  }
}
```

Что означают поля:

- `Url` — адрес источника, к которому надо подключаться
- `Source` — признак биржи, по которому выбирается нужный разбор сообщения
- `Reconnect` — правила повторного подключения именно для этого источника

Если список `Exchange:Connections` отсутствует или пуст, приложение не стартует.

### `Reconnect`

Этот блок лежит внутри каждого подключения.

Поля:

- `MaxAttempts` — сколько раз подряд пробовать подключиться заново  
  Значение `0` означает, что число попыток не ограничено.

- `ConnectTimeoutMs` — сколько ждать установления соединения, после чего считать попытку неудачной

- `DelayMs` — начальная пауза перед повторным подключением

- `MaxDelayMs` — максимальная пауза при увеличении задержки между попытками

- `JitterRatio` — насколько можно случайно сдвигать задержку, чтобы попытки переподключения не шли слишком ровно

### `Batching`

Этот блок управляет пакетной записью в базу.

Поля:

- `BatchSize` — сколько тиков собрать перед записью
- `BatchTimeoutMs` — сколько максимум ждать, даже если пакет еще не набрал нужный размер

Что это меняет по смыслу:

- больший `BatchSize` уменьшает число обращений к базе;
- меньший `BatchTimeoutMs` позволяет быстрее отправлять данные в базу, даже если поток сообщений не очень плотный.

## База данных

Приложение пишет данные в таблицу:

- `trade_ticks`

Основные поля таблицы:

- `id`
- `source`
- `ticker`
- `price`
- `volume`
- `timestamp_utc`

Чтобы не сохранять устойчивые дубликаты, используется уникальность по сочетанию:

- `source`
- `ticker`
- `price`
- `volume`
- `timestamp_utc`

Запись выполняется через:

- `INSERT ... ON CONFLICT DO NOTHING`

## Проверка наличия данных

После запуска тестовых источников и агрегатора можно проверить, что данные действительно пишутся в базу:

```postgresql
select
    count(*) filter (where source = 'binance') as binance_count,
    count(*) filter (where source = 'coinbase') as coinbase_count,
    count(*) filter (where source = 'kraken') as kraken_count
from trade_ticks;
```

Если приложение работает правильно, значения счетчиков должны увеличиваться.

## Тесты

Запуск модульных тестов:

```bash
dotnet test tests/UnitTests/UnitTests.csproj
```
