import requests
import base64
import time
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from loguru import logger
from config import Config

class MoySkladClient:
    """Клиент для работы с API МойСклад"""
    
    def __init__(self, region: str = None, use_test: bool = False):
        self.region = region or Config.REGION
        self.use_test = use_test
        self.login, self.password, self.base_url = Config.get_moysklad_credentials(self.region, self.use_test)
        self.auth_header = self._get_auth_header()
        
        # Настройки лимитов API
        self.rate_limit = Config.MOYSKLAD_RATE_LIMIT
        self.daily_limit = Config.MOYSKLAD_DAILY_LIMIT
        self.min_delay = Config.MOYSKLAD_MIN_DELAY
        
        # Трекинг запросов
        self.request_times = []
        self.daily_requests = 0
        self.last_request_time = 0
        
        access_type = "тестовый" if self.use_test else "основной"
        logger.info(f"Инициализирован клиент МойСклад для региона {self.region} ({access_type} доступ)")
        
    def _get_auth_header(self) -> str:
        """Формирование заголовка авторизации"""
        credentials = f"{self.login}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"
    
    def _check_rate_limit(self):
        """Проверка и соблюдение лимитов API"""
        current_time = time.time()
        
        # Проверяем дневной лимит
        if self.daily_requests >= self.daily_limit:
            logger.warning("Достигнут дневной лимит API МойСклад (1000 запросов)")
            raise Exception("Достигнут дневной лимит API МойСклад")
        
        # Проверяем rate limit (100 запросов в минуту)
        minute_ago = current_time - 60
        self.request_times = [t for t in self.request_times if t > minute_ago]
        
        if len(self.request_times) >= self.rate_limit:
            wait_time = 60 - (current_time - self.request_times[0])
            if wait_time > 0:
                logger.info(f"Rate limit достигнут, ожидание {wait_time:.1f} секунд")
                time.sleep(wait_time)
        
        # Соблюдаем минимальную задержку между запросами
        if self.last_request_time > 0:
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_delay:
                sleep_time = self.min_delay - time_since_last
                logger.debug(f"Задержка {sleep_time:.2f} сек между запросами")
                time.sleep(sleep_time)
        
        # Обновляем трекинг
        self.request_times.append(current_time)
        self.daily_requests += 1
        self.last_request_time = current_time
        
        # Сбрасываем счетчики в начале нового дня
        current_date = date.today()
        if not hasattr(self, '_last_date') or self._last_date != current_date:
            self._last_date = current_date
            self.daily_requests = 0
            logger.info("Сброс дневного счетчика запросов API МойСклад")
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Выполнение HTTP-запроса к API"""
        # Проверяем лимиты перед запросом
        self._check_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": self.auth_header,
            "Accept": "application/json;charset=utf-8",
            "Accept-Encoding": "gzip"
        }
        
        try:
            logger.debug(f"Отправка запроса к МойСклад: {url}")
            logger.debug(f"Параметры: {params}")
            logger.debug(f"Запрос #{self.daily_requests} за сегодня")
            
            # Таймауты: (connect, read)
            response = requests.get(url, headers=headers, params=params, timeout=(5, 60))
            
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code}: {response.text}")
                logger.error(f"URL: {url}")
                logger.error(f"Параметры: {params}")
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка запроса к API МойСклад: {e}")
            raise
    
    def get_contractors_for_today(self) -> List[Dict[str, Any]]:
        """Получение контрагентов, созданных сегодня"""
        # Получаем текущую дату
        today = date.today()
        today_start = f"{today.strftime('%Y-%m-%d')} 00:00:00"
        today_end = f"{today.strftime('%Y-%m-%d')} 23:59:59"
        
        # Фильтр: контрагенты созданные сегодня
        # Формат: created>=2025-08-28 00:00:00;created<=2025-08-28 23:59:59
        filter_str = f"created>={today_start};created<={today_end}"
        
        params = {
            "filter": filter_str,
            "expand": "owner"
        }
        
        try:
            data = self._make_request("/entity/counterparty", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения контрагентов за сегодня: {e}")
            return []
    
    def get_contractors_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Получение контрагентов за указанную дату (для обратной совместимости)"""
        # Форматируем дату для API
        date_str = target_date.strftime("%Y-%m-%d")
        
        # Получаем контрагентов с фильтром по дате создания
        params = {
            "filter": f"moment~={date_str}",
            "expand": "owner",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/counterparty", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения контрагентов: {e}")
            return []
    
    def get_shipments_for_today(self) -> List[Dict[str, Any]]:
        """Получение отгрузок, созданных сегодня"""
        # Для отгрузок используем поле created для фильтрации по дате создания
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"created~={today_str}",
            "expand": "positions,owner,salesChannel,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/demand", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения отгрузок за сегодня: {e}")
            return []
    
    def get_shipments_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Получение отгрузок за указанную дату (для обратной совместимости)"""
        date_str = target_date.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"moment~={date_str}",
            "expand": "positions,owner,salesChannel,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/demand", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения отгрузок: {e}")
            return []
    
    def get_commission_reports_for_today(self) -> List[Dict[str, Any]]:
        """Получение отчетов комиссионеров, созданных сегодня"""
        # Для отчетов комиссионеров используем поле created для фильтрации по дате создания
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"created~={today_str}",
            "expand": "positions,owner,salesChannel,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/commissionreportin", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения отчетов комиссионеров за сегодня: {e}")
            return []
    
    def get_commission_reports_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Получение отчетов комиссионеров за указанную дату (для обратной совместимости)"""
        date_str = target_date.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"moment~={date_str}",
            "expand": "positions,owner,salesChannel,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/commissionreportin", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения отчетов комиссионеров: {e}")
            return []
    
    def get_sales_for_today(self) -> List[Dict[str, Any]]:
        """Получение продаж, созданных сегодня"""
        # Для продаж используем поле created для фильтрации по дате создания
        today = date.today()
        today_str = today.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"created~={today_str}",
            "expand": "positions,owner,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/retaildemand", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения продаж за сегодня: {e}")
            return []
    
    def get_sales_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        """Получение продаж за указанную дату (для обратной совместимости)"""
        date_str = target_date.strftime("%Y-%m-%d")
        
        params = {
            "filter": f"moment~={date_str}",
            "expand": "positions,owner,agent,contract",
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/retaildemand", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения продаж: {e}")
            return []
    
    def get_product_min_prices(self) -> Dict[str, float]:
        """Получение минимальных цен товаров"""
        try:
            data = self._make_request("/entity/product", {"limit": 1000})
            min_prices = {}
            
            for product in data.get("rows", []):
                product_id = product.get("id")
                
                # В МойСклад минимальная цена может быть в разных полях
                min_price = 0
                
                # Проверяем поле minPrice
                if product.get("minPrice") is not None:
                    min_price = float(product.get("minPrice", 0))
                
                # Если нет minPrice, берем из salePrices
                if min_price == 0 and product.get("salePrices"):
                    sale_prices = product.get("salePrices", [])
                    if sale_prices and len(sale_prices) > 0:
                        # Цена в копейках, делим на 100
                        price_value = sale_prices[0].get("value", 0)
                        if isinstance(price_value, (int, float)) and price_value > 0:
                            min_price = float(price_value) / 100
                
                if min_price > 0:
                    min_prices[product_id] = min_price
            
            return min_prices
        except Exception as e:
            logger.error(f"Ошибка получения минимальных цен товаров: {e}")
            return {}
    
    def get_products_by_price(self, price_condition: str, price_value: float) -> List[Dict[str, Any]]:
        """
        Получение товаров по условию цены
        
        Args:
            price_condition: Условие сравнения ('>', '<', '>=', '<=', '=', '!=')
            price_value: Значение цены (в рублях)
        
        Returns:
            Список товаров, соответствующих условию
        """
        # Конвертируем цену в копейки для API МойСклад
        price_kopecks = int(price_value * 100)
        
        # Формируем фильтр по цене
        price_filter = f"salePrices.value{price_condition}{price_kopecks}"
        
        params = {
            "filter": price_filter,
            "limit": 1000
        }
        
        try:
            data = self._make_request("/entity/product", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения товаров по цене {price_condition} {price_value}: {e}")
            return []
    
    def get_products_with_zero_price(self) -> List[Dict[str, Any]]:
        """Получение товаров с нулевой ценой"""
        return self.get_products_by_price("=", 0)
    
    def get_products_below_price(self, max_price: float) -> List[Dict[str, Any]]:
        """Получение товаров с ценой ниже указанной"""
        return self.get_products_by_price("<", max_price)
    
    def get_products_above_price(self, min_price: float) -> List[Dict[str, Any]]:
        """Получение товаров с ценой выше указанной"""
        return self.get_products_by_price(">", min_price)
    
    def get_api_stats(self) -> Dict[str, Any]:
        """Получение статистики использования API"""
        current_time = time.time()
        minute_ago = current_time - 60
        
        # Запросы за последнюю минуту
        requests_last_minute = len([t for t in self.request_times if t > minute_ago])
        
        # Оставшиеся запросы
        remaining_daily = self.daily_limit - self.daily_requests
        remaining_minute = self.rate_limit - requests_last_minute
        
        return {
            "daily_requests": self.daily_requests,
            "daily_limit": self.daily_limit,
            "remaining_daily": remaining_daily,
            "requests_last_minute": requests_last_minute,
            "minute_limit": self.rate_limit,
            "remaining_minute": remaining_minute,
            "last_request_time": self.last_request_time,
            "min_delay": self.min_delay
        }
    
    def reset_counters(self):
        """Сброс счетчиков запросов (для тестирования)"""
        self.request_times = []
        self.daily_requests = 0
        self.last_request_time = 0
        logger.info("Счетчики API МойСклад сброшены")

    def get_custom_entity_metadata(self, custom_entity_id: str) -> Dict[str, Any]:
        """Получение метаданных пользовательского справочника"""
        try:
            data = self._make_request(f"/context/companysettings/metadata/customEntities/{custom_entity_id}")
            return data
        except Exception as e:
            logger.error(f"Ошибка получения метаданных справочника {custom_entity_id}: {e}")
            return {}
    
    def get_custom_entity_values(self, custom_entity_id: str) -> List[Dict[str, Any]]:
        """Получение значений пользовательского справочника"""
        try:
            data = self._make_request(f"/entity/customentity/{custom_entity_id}")
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения значений справочника {custom_entity_id}: {e}")
            return []
    
    def find_pd_agreement_field(self, contractor: Dict[str, Any]) -> Dict[str, Any]:
        """Поиск поля 'Соглашение политики ПД' в контрагенте"""
        # Ищем в customFields
        custom_fields = contractor.get("customFields", [])
        
        for field in custom_fields:
            field_name = field.get("name", "")
            if "Соглашение политики ПД" in field_name:
                return {
                    "field_name": field_name,
                    "field_value": field.get("value"),
                    "field_type": "customField",
                    "location": "customFields"
                }
        
        # Ищем в других полях контрагента
        for key, value in contractor.items():
            if "соглашение" in key.lower() and "пд" in key.lower():
                return {
                    "field_name": key,
                    "field_value": value,
                    "field_type": "direct_field",
                    "location": key
                }
        
        return {}
    
    def get_contractors_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение контрагентов за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str
        }
        
        try:
            data = self._make_request("/entity/counterparty", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения контрагентов за период {start_date} - {end_date}: {e}")
            return []
    
    def get_shipments_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение отгрузок за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "owner,salesChannel,agent,contract"
        }
        
        try:
            data = self._make_request("/entity/demand", params)
            shipments = data.get("rows", [])
            
            # Загружаем позиции для каждой отгрузки
            for shipment in shipments:
                shipment_id = shipment.get("id")
                if shipment_id:
                    try:
                        positions_data = self._make_request(f"/entity/demand/{shipment_id}/positions")
                        shipment["positions"] = positions_data
                    except Exception as e:
                        logger.warning(f"Не удалось загрузить позиции для отгрузки {shipment.get('name')}: {e}")
                        shipment["positions"] = {"rows": []}
            
            return shipments
        except Exception as e:
            logger.error(f"Ошибка получения отгрузок за период {start_date} - {end_date}: {e}")
            return []
    
    def get_commission_reports_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение отчетов комиссионеров за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "positions,owner,salesChannel,agent,contract"
        }
        
        try:
            data = self._make_request("/entity/commissionreportin", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения отчетов комиссионеров за период {start_date} - {end_date}: {e}")
            return []
    
    def get_sales_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение продаж за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "positions,owner,salesChannel,agent"
        }
        
        try:
            data = self._make_request("/entity/retaildemand", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения продаж за период {start_date} - {end_date}: {e}")
            return []
