"""
Универсальный скрипт для запуска мониторинга МойСклад
Использование:
    python run_monitoring.py --region RB --document shipments --date-from 2025-09-01 --date-to 2025-09-09
    python run_monitoring.py --region RB --document contractors --date 2025-09-01
    python run_monitoring.py --help
"""
import argparse
from datetime import date, datetime, timedelta
from monitoring_service_v2 import MonitoringServiceV2
from telegram_bot import TelegramMonitoringBot
from loguru import logger

# Настройка логирования
logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{time:HH:mm:ss} | {level} | {message}", level="INFO")


def main():
    parser = argparse.ArgumentParser(description='Мониторинг документов МойСклад')
    
    parser.add_argument('--region', type=str, default='RB', choices=['RB', 'RF', 'KZ'],
                        help='Регион: RB (BY-Беларусь), RF (RU-Россия), KZ (Казахстан)')
    
    parser.add_argument('--document', type=str, required=True,
                        choices=['shipments', 'contractors', 'sales', 'commission'],
                        help='Тип документа: shipments, contractors, sales, commission')
    
    parser.add_argument('--date-from', type=str, help='Дата начала периода (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=str, help='Дата окончания периода (YYYY-MM-DD)')
    parser.add_argument('--date', type=str, help='Одна дата (альтернатива date-from и date-to)')
    
    parser.add_argument('--send-to-bitrix', action='store_true', help='Отправить отчет в Bitrix24')
    parser.add_argument('--detailed', action='store_true', help='Детализированный отчет')
    
    args = parser.parse_args()
    
    # Определяем период
    if args.date:
        date_from = datetime.strptime(args.date, '%Y-%m-%d').date()
        date_to = date_from
    elif args.date_from and args.date_to:
        date_from = datetime.strptime(args.date_from, '%Y-%m-%d').date()
        date_to = datetime.strptime(args.date_to, '%Y-%m-%d').date()
    elif args.date_from:
        date_from = datetime.strptime(args.date_from, '%Y-%m-%d').date()
        date_to = date_from
    else:
        # По умолчанию - вчера
        date_from = date.today() - timedelta(days=1)
        date_to = date_from
    
    # Маппинг регионов
    region_names = {
        'RB': 'BY (Беларусь)',
        'RF': 'RU (Россия)',
        'KZ': 'KZ (Казахстан)'
    }
    
    document_names = {
        'shipments': 'Отгрузки',
        'contractors': 'Контрагенты',
        'sales': 'Продажи',
        'commission': 'Отчеты комиссионеров'
    }
    
    print("="*80)
    print(f"🔍 МОНИТОРИНГ МОЙСКЛАД")
    print("="*80)
    print(f"Регион: {region_names.get(args.region, args.region)}")
    print(f"Документ: {document_names.get(args.document, args.document)}")
    print(f"Период: {date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}")
    print("="*80)
    
    # Инициализация сервиса
    service = MonitoringServiceV2(region=args.region)
    
    # Запуск проверки
    result = None
    if args.document == 'shipments':
        result = service.check_shipments_period(date_from, date_to)
    elif args.document == 'contractors':
        result = service.check_contractors_period(date_from, date_to)
    elif args.document == 'sales':
        result = service.check_sales_period(date_from, date_to)
    elif args.document == 'commission':
        result = service.check_commission_reports_period(date_from, date_to)
    
    if not result:
        print("❌ Ошибка выполнения проверки")
        return
    
    # Вывод результатов
    print("="*80)
    print(f"\n📊 РЕЗУЛЬТАТЫ:")
    print(f"Всего: {result.get('total', 0)}")
    print(f"Валидных: {result.get('valid', 0)}")
    print(f"С ошибками: {len(result.get('errors', []))}")
    
    if result.get('errors'):
        print(f"\n❌ Найдено {len(result['errors'])} документов с ошибками")
        
        if args.detailed:
            print("\nДетализация:")
            for i, error in enumerate(result['errors'], 1):
                name = error.get('name', 'Без названия')
                owner = error.get('owner', 'Не указан')
                owner_display = owner

                print(f"\n{i}. {name} — {owner_display}")

                moment = error.get('moment')
                if moment:
                    try:
                        dt = datetime.fromisoformat(str(moment).replace('Z', ''))
                        print(f"   📅 {dt.strftime('%d.%m.%Y %H:%M')}")
                    except Exception:
                        print(f"   📅 {moment}")

                link = error.get('link')
                if link:
                    print(f"   🔗 {link}")

                issues = TelegramMonitoringBot._extract_issues(error)
                if issues:
                    for issue in issues:
                        print(f"   - {issue}")
                else:
                    print("   - Без описания")
    else:
        print("\n✅ Все документы в порядке!")
    
    # Отправка в Bitrix24
    if args.send_to_bitrix:
        print("\n" + "="*80)
        print("📤 Отправка отчета в Bitrix24...")
        
        message, excel_path = TelegramMonitoringBot._format_bitrix_message(
            args.document,
            args.region,
            date_from,
            date_to,
            result
        )

        try:
            service.bitrix24_client.send_message_to_chat(message)
            if excel_path:
                service.bitrix24_client.send_file_to_chat(excel_path, "📎 Полный список ошибок")
            print("✅ Отчет успешно отправлен в Bitrix24!")
        except Exception as e:
            print(f"❌ Ошибка отправки: {e}")
        finally:
            if excel_path:
                try:
                    excel_path.unlink()
                except Exception:
                    pass
    
    print("="*80)


if __name__ == '__main__':
    main()



