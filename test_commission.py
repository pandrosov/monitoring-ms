from datetime import date, timedelta
from monitoring_service_v2 import MonitoringServiceV2
from loguru import logger

logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

# Проверяем отчеты комиссионеров за последнюю неделю
service = MonitoringServiceV2(region="RB")
end_date = date.today()
start_date = end_date - timedelta(days=6)

print(f"🔍 Запуск проверки ОТЧЕТОВ КОМИССИОНЕРОВ для региона BY (Беларусь)")
print(f"   Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}")
print("="*80)

result = service.check_commission_reports_period(start_date, end_date)

print("="*80)
print(f"\n📊 Результаты:")
print(f"Всего отчетов: {result.get('total', 0)}")
print(f"Валидных: {result.get('valid', 0)}")
print(f"С ошибками: {len(result.get('errors', []))}")

if result.get('errors'):
    print(f"\n❌ Найдены ошибки в {len(result['errors'])} отчетах:")
    
    # Статистика по типам
    stats = {
        'price_errors': 0,
        'channel_error': 0,
        'project_error': 0,
        'contract_error': 0
    }
    
    for error in result['errors']:
        if error.get('price_errors'):
            stats['price_errors'] += 1
        if error.get('channel_error'):
            stats['channel_error'] += 1
        if error.get('project_error'):
            stats['project_error'] += 1
        if error.get('contract_error'):
            stats['contract_error'] += 1
    
    print(f"\n📈 Статистика:")
    if stats['price_errors']:
        print(f"   💰 Проблемы с ценами: {stats['price_errors']}")
    if stats['channel_error']:
        print(f"   📢 Канал продаж: {stats['channel_error']}")
    if stats['project_error']:
        print(f"   🏗️ Проект: {stats['project_error']}")
    if stats['contract_error']:
        print(f"   📄 Договор: {stats['contract_error']}")
    
    print(f"\n📋 Примеры (первые 3):")
    for i, error in enumerate(result['errors'][:3], 1):
        print(f"\n{i}. {error.get('name')} - Владелец: {error.get('owner', 'N/A')}")
        if error.get('price_errors'):
            print(f"   💰 Нулевые цены: {len(error['price_errors'])} товаров")
        if error.get('channel_error'):
            print(f"   📢 {error['channel_error']}")
        if error.get('project_error'):
            print(f"   🏗️ {error['project_error']}")
        if error.get('contract_error'):
            print(f"   📄 {error['contract_error']}")
else:
    print("\n✅ Все отчеты комиссионеров в порядке!")

print("\n" + "="*80)





