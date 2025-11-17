import base64
import time
from collections import deque
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import requests
from loguru import logger
from config import Config

class MoySkladClient:
    """Клиент для работы с API МойСклад"""
    
    def __init__(self, region: str = None, use_test: bool = False):
        self.region = region or Config.REGION
        self.use_test = use_test
        self.login, self.password, self.base_url = Config.get_moysklad_credentials(self.region, self.use_test)
        self.auth_header = self._get_auth_header()
        
        # Настройки задержки между запросами
        self.min_delay = Config.MOYSKLAD_MIN_DELAY
        self.max_retry_429 = 5
        self.last_request_time = 0.0

        # Мониторинг ошибок
        self.error_window_seconds = 60
        self.error_events = deque()  # (timestamp, status_code, endpoint)
        
        access_type = "тестовый" if self.use_test else "основной"
        logger.info(f"Инициализирован клиент МойСклад для региона {self.region} ({access_type} доступ)")
        
    def _get_auth_header(self) -> str:
        """Формирование заголовка авторизации"""
        credentials = f"{self.login}:{self.password}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"
    
    def _apply_request_delay(self):
        """Минимальная задержка между запросами, чтобы снизить риск 429."""
        if self.min_delay <= 0:
            return

        if self.last_request_time > 0:
            time_since_last = time.time() - self.last_request_time
            if time_since_last < self.min_delay:
                sleep_time = self.min_delay - time_since_last
                logger.debug(f"Задержка {sleep_time:.2f} сек между запросами")
                time.sleep(sleep_time)
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Выполнение HTTP-запроса к API"""
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": self.auth_header,
            "Accept": "application/json;charset=utf-8",
            "Accept-Encoding": "gzip"
        }
        
        attempt = 0

        while True:
            try:
                # Соблюдаем минимальную задержку между запросами
                self._apply_request_delay()

                logger.debug(f"Отправка запроса к МойСклад: {url}")
                logger.debug(f"Параметры: {params}")
                logger.debug(f"Попытка {attempt + 1}")

                response = requests.get(url, headers=headers, params=params, timeout=(5, 60))
                self.last_request_time = time.time()

                if response.status_code == 200:
                    self._prune_error_events()
                    return response.json()

                # Лимит запросов
                if response.status_code == 429:
                    self._register_error(response.status_code, endpoint)
                    attempt += 1
                    if attempt > self.max_retry_429:
                        logger.error("Превышено число попыток повторного запроса после 429")
                        response.raise_for_status()

                    wait_time = self._calculate_retry_delay(response, attempt)
                    logger.warning(
                        f"Получен ответ 429 Too Many Requests. Ожидание {wait_time:.1f} сек перед повтором."
                    )
                    time.sleep(wait_time)
                    continue

                # Другие ошибки
                if response.status_code >= 400:
                    self._register_error(response.status_code, endpoint)
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    logger.error(f"URL: {url}")
                    logger.error(f"Параметры: {params}")
                    response.raise_for_status()

                # На всякий случай
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка запроса к API МойСклад: {e}")
                raise

    def _prune_error_events(self):
        """Удаление устаревших записей об ошибках."""
        now = time.time()
        while self.error_events and now - self.error_events[0][0] > self.error_window_seconds:
            self.error_events.popleft()

    def _register_error(self, status_code: int, endpoint: str):
        """Логирование и контроль числа ошибок, чтобы не попасть под автоматическое отключение."""
        now = time.time()
        self.error_events.append((now, status_code, endpoint))
        self._prune_error_events()

        # Подсчитываем ошибки за минуту по статусу и endpoint
        total_errors_last_minute = len(self.error_events)
        similar_errors = [
            event
            for event in self.error_events
            if event[1] == status_code and event[2] == endpoint
        ]

        if total_errors_last_minute >= 150:
            logger.warning(
                "За последнюю минуту зафиксировано {} ошибок. "
                "Проверьте корректность запросов, чтобы избежать блокировки API.",
                total_errors_last_minute
            )

        similar_count = len(similar_errors)
        if similar_count >= 180:
            logger.warning(
                "За последнюю минуту зафиксировано {} ошибок со статусом {} для ресурса {}. "
                "Приближаемся к порогу автоматического отключения.",
                similar_count,
                status_code,
                endpoint
            )

    @staticmethod
    def _calculate_retry_delay(response: requests.Response, attempt: int) -> float:
        """Определение задержки перед повтором после 429."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 1.0)
            except ValueError:
                pass

        # экспоненциальная задержка с верхним пределом
        return min(30.0, 2 ** attempt)
    
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
        except requests.exceptions.HTTPError as e:
            error_msg = str(e)
            if "429" in error_msg or "лимит" in error_msg.lower() or "limit" in error_msg.lower():
                logger.warning(f"Достигнут дневной лимит API МойСклад при получении контрагентов за период {start_date} - {end_date}")
                raise RuntimeError("Достигнут дневной лимит API МойСклад (1000 запросов). Попробуйте позже.")
            logger.error(f"Ошибка получения контрагентов за период {start_date} - {end_date}: {e}")
            raise
        except Exception as e:
            error_msg = str(e)
            if "лимит" in error_msg.lower() or "limit" in error_msg.lower():
                logger.warning(f"Достигнут дневной лимит API МойСклад при получении контрагентов за период {start_date} - {end_date}")
                raise RuntimeError("Достигнут дневной лимит API МойСклад (1000 запросов). Попробуйте позже.")
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
        except requests.exceptions.HTTPError as e:
            error_msg = str(e)
            if "429" in error_msg or "лимит" in error_msg.lower() or "limit" in error_msg.lower():
                logger.warning(f"Достигнут дневной лимит API МойСклад при получении отгрузок за период {start_date} - {end_date}")
                raise RuntimeError("Достигнут дневной лимит API МойСклад (1000 запросов). Попробуйте позже.")
            logger.error(f"Ошибка получения отгрузок за период {start_date} - {end_date}: {e}")
            raise
        except RuntimeError:
            # Пробрасываем RuntimeError с сообщением о лимите
            raise
        except Exception as e:
            error_msg = str(e)
            if "лимит" in error_msg.lower() or "limit" in error_msg.lower():
                logger.warning(f"Достигнут дневной лимит API МойСклад при получении отгрузок за период {start_date} - {end_date}")
                raise RuntimeError("Достигнут дневной лимит API МойСклад (1000 запросов). Попробуйте позже.")
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
    
    def get_sales_returns_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение возвратов покупателей за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "positions,owner,salesChannel,agent,contract"
        }
        
        try:
            data = self._make_request("/entity/salesreturn", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения возвратов покупателей за период {start_date} - {end_date}: {e}")
            return []
    
    def get_retail_returns_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение возвратов розницы за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "positions,owner,salesChannel,agent"
        }
        
        try:
            data = self._make_request("/entity/retailsalesreturn", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения возвратов розницы за период {start_date} - {end_date}: {e}")
            return []
    
    def get_commission_returns_for_period(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """Получение возвратов отчетов комиссионеров за период"""
        start_str = f"{start_date.strftime('%Y-%m-%d')} 00:00:00"
        end_str = f"{end_date.strftime('%Y-%m-%d')} 23:59:59"
        
        filter_str = f"created>={start_str};created<={end_str}"
        
        params = {
            "filter": filter_str,
            "expand": "positions,owner,salesChannel,agent,contract"
        }
        
        try:
            data = self._make_request("/entity/commissionreportout", params)
            return data.get("rows", [])
        except Exception as e:
            logger.error(f"Ошибка получения возвратов комиссионеров за период {start_date} - {end_date}: {e}")
            return []
