from datetime import date, timedelta, datetime
from monitoring_service_v2 import MonitoringServiceV2
from loguru import logger

logger.remove()
logger.add(lambda msg: print(msg, end=""), format="{time:HH:mm:ss} | {level} | {message}", level="WARNING")

# Проверяем продажи за вчера
service = MonitoringServiceV2(region="RB")
check_date = date.today() - timedelta(days=1)

print(f"🔍 Проверка продаж BY за {check_date.strftime('%d.%m.%Y')}")
print("="*80)

result = service.check_sales_period(check_date, check_date)

print(f"\n📊 Всего: {result.get('total', 0)}, Валидных: {result.get('valid', 0)}, Ошибок: {len(result.get('errors', []))}")
print("="*80)

# Формируем отчет для Bitrix24
message = f"💰 Мониторинг продаж BY за {check_date.strftime('%d.%m.%Y')}\n\n"
message += f"📊 Статистика:\n"
message += f"Всего: {result.get('total', 0)}, "
message += f"Валидных: {result.get('valid', 0)}, "
message += f"Ошибок: {len(result.get('errors', []))}\n"

if result.get('errors'):
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
    
    message += f"\n📈 Типы ошибок:\n"
    if stats['channel_error']:
        message += f"📢 Канал продаж: {stats['channel_error']}\n"
    if stats['price_errors']:
        message += f"💰 Цены: {stats['price_errors']}\n"
    if stats['source_error']:
        message += f"🎯 Источник продажи: {stats['source_error']}\n"
    if stats['project_error']:
        message += f"🏗️ Проект: {stats['project_error']}\n"
    
    message += f"\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"📋 Детализация (первые 10):\n\n"
    
    for i, error in enumerate(result['errors'][:10], 1):
        name = error.get('name', 'Без названия')
        owner = error.get('owner', 'N/A')
        moment = error.get('moment', '')
        sale_id = error.get('id', '')
        
        # Дата
        date_str = ""
        if moment:
            try:
                dt = datetime.fromisoformat(moment.replace("Z", ""))
                date_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                date_str = moment[:16] if len(moment) >= 16 else moment
        
        message += f"{i}. {name}"
        if date_str:
            message += f" ({date_str})"
        message += f"\n   👤 {owner}\n"
        
        # Ссылка
        if sale_id:
            message += f"   🔗 https://online.moysklad.ru/app/#retaildemand/edit?id={sale_id}\n"
        
        # Причины
        message += f"   ❌ "
        reasons = []
        if error.get('channel_error'):
            reasons.append(error['channel_error'])
        if error.get('price_errors'):
            reasons.append(f"Нулевые цены: {len(error['price_errors'])} товаров")
        if error.get('source_error'):
            reasons.append(error['source_error'])
        if error.get('project_error'):
            reasons.append(error['project_error'])
        
        message += "; ".join(reasons) + "\n\n"
    
    if len(result['errors']) > 10:
        message += f"... и еще {len(result['errors']) - 10} продаж\n"
else:
    message += "\n✅ Все продажи в порядке!\n"

# Показываем предпросмотр
print("\n📋 ПРЕДПРОСМОТР ОТЧЕТА:\n")
print(message)
print("="*80)

# Отправляем в Bitrix24
print("\n📤 Отправка в Bitrix24...")
try:
    service.bitrix24_client.send_message_to_chat(message)
    print("✅ Отчет успешно отправлен!")
except Exception as e:
    print(f"❌ Ошибка: {e}")

print("="*80)





