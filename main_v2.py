#!/usr/bin/env python3
"""
Сервис мониторинга МойСклад v2
Отправляет уведомления в Битрикс24 о проблемах с документами
Поддерживает регионы РБ и РФ
"""

import os
import schedule
import time
from datetime import date, timedelta
from loguru import logger
from config import Config
from monitoring_service_v2 import MonitoringServiceV2

def setup_logging():
    """Настройка логирования"""
    logger.remove()
    # Обеспечиваем наличие папки для логов
    try:
        os.makedirs("logs", exist_ok=True)
    except Exception:
        pass
    logger.add(
        "logs/monitoring.log",
        rotation="1 day",
        retention="30 days",
        level=Config.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level=Config.LOG_LEVEL,
        format="{time:HH:mm:ss} | {level} | {message}"
    )

def run_monitoring():
    """Запуск мониторинга документов за вчерашний день"""
    try:
        service = MonitoringServiceV2()
        yesterday = date.today() - timedelta(days=1)
        success = service.run_monitoring(yesterday, yesterday)
        
        if success:
            logger.info(f"Мониторинг документов за {yesterday.strftime('%d.%m.%Y')} успешно завершен")
        else:
            logger.error(f"Мониторинг документов за {yesterday.strftime('%d.%m.%Y')} завершился с ошибками")
            
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске мониторинга: {e}")

def run_shipments_week():
    """Запуск проверки отгрузок за последнюю неделю (включая сегодня) без отправки в Битрикс"""
    try:
        service = MonitoringServiceV2()
        end_date = date.today()
        start_date = end_date - timedelta(days=6)
        logger.info(f"Запуск проверки отгрузок за неделю: {start_date} - {end_date}")
        result = service.check_shipments_period(start_date, end_date)
        if result.get("status") == "success":
            logger.info(
                f"Итог по отгрузкам: Всего={result.get('total', 0)}, "
                f"Валидных={result.get('valid', 0)}, Ошибок={len(result.get('errors', []))}"
            )
        else:
            logger.error("Проверка отгрузок завершилась с ошибкой")
    except Exception as e:
        logger.error(f"Критическая ошибка при проверке отгрузок за неделю: {e}")

def run_monitoring_for_date(target_date_str: str):
    """Запуск мониторинга для конкретной даты"""
    try:
        # Парсим дату из строки (формат: YYYY-MM-DD)
        target_date = date.fromisoformat(target_date_str)
        service = MonitoringServiceV2()
        success = service.run_monitoring(target_date, target_date)
        
        if success:
            logger.info(f"Мониторинг за {target_date_str} успешно завершен")
        else:
            logger.error(f"Мониторинг за {target_date_str} завершился с ошибками")
            
    except ValueError:
        logger.error(f"Неверный формат даты: {target_date_str}. Используйте формат YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске мониторинга за {target_date_str}: {e}")

def run_monitoring_for_period(start_date_str: str, end_date_str: str):
    """Запуск мониторинга для периода"""
    try:
        # Парсим даты из строк (формат: YYYY-MM-DD)
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
        service = MonitoringServiceV2()
        success = service.run_monitoring(start_date, end_date)
        
        if success:
            logger.info(f"Мониторинг за период {start_date_str} - {end_date_str} успешно завершен")
        else:
            logger.error(f"Мониторинг за период {start_date_str} - {end_date_str} завершился с ошибками")
            
    except ValueError:
        logger.error(f"Неверный формат даты. Используйте формат YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске мониторинга за период: {e}")

def main():
    """Основная функция"""
    setup_logging()
    
    # Проверяем конфигурацию
    try:
        Config.validate()
        logger.info("Конфигурация загружена успешно")
        logger.info(f"Регион: {Config.REGION}")
        logger.info(f"Контакт-Центр: {Config.CONTACT_CENTER_EMPLOYEE}")
        
        # Показываем информацию о регионе
        moysklad_login, _, moysklad_url = Config.get_moysklad_credentials()
        logger.info(f"МойСклад: {moysklad_url} (логин: {moysklad_login})")
        logger.info(f"Битрикс24: {Config.BITRIX24_WEBHOOK_URL} (чат: {Config.BITRIX24_CHAT_ID})")
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
        logger.error("Проверьте файл .env и настройте обязательные параметры")
        return
    
    # Настраиваем расписание
    schedule.every().day.at("09:00").do(run_monitoring)  # Ежедневно в 9:00
    
    logger.info("Сервис мониторинга МойСклад v2 запущен")
    logger.info("Мониторинг документов за вчерашний день будет выполняться ежедневно в 9:00")
    logger.info("Для запуска мониторинга за конкретную дату используйте: python main_v2.py --date YYYY-MM-DD")
    logger.info("Для запуска мониторинга за период используйте: python main_v2.py --period YYYY-MM-DD YYYY-MM-DD")
    logger.info("Для проверки отгрузок за последнюю неделю: python main_v2.py --shipments-week")
    
    # Если передан аргумент --date, запускаем мониторинг для указанной даты
    import sys
    if len(sys.argv) > 2 and sys.argv[1] == "--date":
        target_date = sys.argv[2]
        logger.info(f"Запуск мониторинга за {target_date}")
        run_monitoring_for_date(target_date)
        return
    
    # Если передан аргумент --period, запускаем мониторинг для указанного периода
    if len(sys.argv) > 3 and sys.argv[1] == "--period":
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        logger.info(f"Запуск мониторинга за период {start_date} - {end_date}")
        run_monitoring_for_period(start_date, end_date)
        return

    # Если передан аргумент --shipments-week, выполняем только аналитику отгрузок за последнюю неделю
    if len(sys.argv) > 1 and sys.argv[1] == "--shipments-week":
        run_shipments_week()
        return
    
    # Основной цикл планировщика
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Проверяем каждую минуту
    except KeyboardInterrupt:
        logger.info("Сервис остановлен пользователем")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")

if __name__ == "__main__":
    main()
