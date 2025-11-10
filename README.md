# Мониторинг документов МойСклад

Автоматизированный контроль документов МойСклад для трёх регионов: Беларусь (РБ), Россия (РФ) и Казахстан (КЗ). Система проверяет отгрузки, розничные продажи, отчёты комиссионеров и карточки контрагентов, формируя отчёты и при необходимости отправляя результаты в Bitrix24 и Telegram.

## Возможности

- единый сервис проверки документов по расписанию или вручную;
- поддержка региональных правил (телефоны, реквизиты, оплату и т.д.);
- детализированные отчёты с разбивкой по владельцам;
- Telegram-бот с доступом по списку пользователей;
- интеграция с Bitrix24 для доставки результатов.

Подробная схема проверок — в `docs/overview.md`.

## Быстрый старт

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp example.env .env
# заполните креденциалы МойСклад, Bitrix24 и Telegram
```

Основные параметры `.env`:

```bash
MOYSKLAD_LOGIN_BY=admin@company
MOYSKLAD_PASSWORD_BY=*****
MOYSKLAD_LOGIN_RU=admin@company
MOYSKLAD_PASSWORD_RU=*****
MOYSKLAD_LOGIN_KZ=admin@company
MOYSKLAD_PASSWORD_KZ=*****

BITRIX24_WEBHOOK_URL=https://example.bitrix24.ru/rest/1/webhook/
BITRIX24_CHAT_ID=chat123

TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_ALLOWED_USERS="11111111,22222222"
```

## Запуск

```bash
# Проверка за период
python run_monitoring.py --region RB --document shipments --date-from 2025-09-01 --date-to 2025-09-07

# Пример запуска (main_v2.py)
python main_v2.py

# Telegram-бот
python telegram_bot.py
```

### Docker / Makefile

```bash
make build                   # сборка образа
make run                     # запуск основного сценария
make monitor ARGS="--region RB --document shipments --date-from 2025-09-01 --date-to 2025-09-07"
make bot                     # запуск Telegram-бота
make shell                   # bash в контейнере

# запуск в фоне
make run DETACH=1
```

## Telegram-бот

Бот позволяет запускать проверки, просматривать результаты и отправлять сводки в Bitrix24. Доступ выдаётся по Telegram ID. Полное руководство: `docs/telegram.md`.

## Структура проекта

```
.
├── bitrix24_client.py        # интеграция с Bitrix24
├── config.py                 # конфигурация и доступы
├── monitoring_service_v2.py  # основная логика проверок
├── moysklad_client.py        # клиент API МойСклад
├── telegram_bot.py           # Telegram-бот
├── run_monitoring.py         # CLI для проверок
├── main_v2.py                # пример запуска
├── docs/
│   ├── overview.md
│   └── telegram.md
├── requirements.txt
├── Makefile
├── Dockerfile
└── example.env
```

## Документация

- `docs/overview.md` — назначение, проверки по регионам, варианты запуска.
- `docs/telegram.md` — руководство по Telegram-боту и правила доступа.
- `example.env` — образец конфига.

## Безопасность

- храните токены и пароли только в `.env` (файл уже в `.gitignore`);
- ограничивайте доступ к Telegram-боту через переменную `TELEGRAM_ALLOWED_USERS`;
- не публикуйте отчёты и логи в репозитории (директории игнорируются).

## Поддержка

- проверка доступа/кредов: `python run_monitoring.py --help`;
- диагностика бота: `tail -f logs/telegram_bot.log` (если включено логирование);
- вопросы по доступу — к ответственному за проект.
