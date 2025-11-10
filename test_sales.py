from datetime import date, timedelta
from monitoring_service_v2 import MonitoringServiceV2
from loguru import logger

logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{time:HH:mm:ss} | {level} | {message}", level="INFO")

# Проверяем продажи за вчера
service = MonitoringServiceV2(region="RB")
check_date = date.today() - timedelta(days=1)

print(f"🔍 Запуск проверки ПРОДАЖ для региона BY (Беларусь)")
print(f"   Дата: {check_date.strftime('%d.%m.%Y')}")
print("="*80)

result = service.check_sales_period(check_date, check_date)

print("="*80)
print(f"\n📊 Результаты за {check_date.strftime('%d.%m.%Y')}:")
print(f"Всего продаж: {result.get('total', 0)}")
print(f"Валидных: {result.get('valid', 0)}")
print(f"С ошибками: {len(result.get('errors', []))}")

if result.get('errors'):
    print(f"\n❌ Найдены ошибки в {len(result['errors'])} продажах:")
    
    # Статистика по типам
    stats = {'channel_error': 0, 'price_errors': 0, 'source_error': 0, 'project_error': 0}
    for error in result['errors']:
        if error.get('channel_error'):
            stats['channel_error'] += 1
        if error.get('price_errors'):
            stats['price_errors'] += 1
        if error.get('source_error'):
            stats['source_error'] += 1
        if error.get('project_error'):
            stats['project_error'] += 1
    
    print(f"\n📈 Статистика:")
    if stats['channel_error']:
        print(f"   📢 Канал продаж: {stats['channel_error']}")
    if stats['price_errors']:
        print(f"   💰 Проблемы с ценами: {stats['price_errors']}")
    if stats['source_error']:
        print(f"   🎯 Источник продажи: {stats['source_error']}")
    if stats['project_error']:
        print(f"   🏗️ Проект: {stats['project_error']}")
    
    print(f"\n📋 Примеры (первые 3):")
    for i, error in enumerate(result['errors'][:3], 1):
        print(f"\n{i}. {error.get('name')} - Сотрудник: {error.get('owner', 'N/A')}")
        if error.get('channel_error'):
            print(f"   📢 {error['channel_error']}")
        if error.get('price_errors'):
            print(f"   💰 Нулевые цены: {len(error['price_errors'])} товаров")
        if error.get('source_error'):
            print(f"   🎯 {error['source_error']}")
        if error.get('project_error'):
            print(f"   🏗️ {error['project_error']}")
else:
    print("\n✅ Все продажи в порядке!")

print("\n" + "="*80)

