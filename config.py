import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Настройки регионов
    REGION = os.getenv("REGION", "RB")  # RB или RF
    CONTACT_CENTER_EMPLOYEE = os.getenv("CONTACT_CENTER_EMPLOYEE", "Контакт-Центр")
    
    # МойСклад настройки по регионам
    # Поддерживаем оба формата: MOYSKLAD_BY_LOGIN и MOYSKLAD_LOGIN_BY
    
    # Беларусь (BY)
    MOYSKLAD_BY_LOGIN = os.getenv("MOYSKLAD_BY_LOGIN") or os.getenv("MOYSKLAD_LOGIN_BY")
    MOYSKLAD_BY_PASSWORD = os.getenv("MOYSKLAD_BY_PASSWORD") or os.getenv("MOYSKLAD_PASSWORD_BY")
    MOYSKLAD_BY_BASE_URL = os.getenv("MOYSKLAD_BY_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
    
    # Россия (RU)
    MOYSKLAD_RU_LOGIN = os.getenv("MOYSKLAD_RU_LOGIN") or os.getenv("MOYSKLAD_LOGIN_RU")
    MOYSKLAD_RU_PASSWORD = os.getenv("MOYSKLAD_RU_PASSWORD") or os.getenv("MOYSKLAD_PASSWORD_RU")
    MOYSKLAD_RU_BASE_URL = os.getenv("MOYSKLAD_RU_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
    
    # Казахстан (KZ)
    MOYSKLAD_KZ_LOGIN = os.getenv("MOYSKLAD_KZ_LOGIN") or os.getenv("MOYSKLAD_LOGIN_KZ")
    MOYSKLAD_KZ_PASSWORD = os.getenv("MOYSKLAD_KZ_PASSWORD") or os.getenv("MOYSKLAD_PASSWORD_KZ")
    MOYSKLAD_KZ_BASE_URL = os.getenv("MOYSKLAD_KZ_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
    
    # Обратная совместимость (старое именование RB/RF)
    MOYSKLAD_RB_LOGIN = os.getenv("MOYSKLAD_RB_LOGIN") or MOYSKLAD_BY_LOGIN
    MOYSKLAD_RB_PASSWORD = os.getenv("MOYSKLAD_RB_PASSWORD") or MOYSKLAD_BY_PASSWORD
    MOYSKLAD_RB_BASE_URL = os.getenv("MOYSKLAD_RB_BASE_URL", MOYSKLAD_BY_BASE_URL or "https://api.moysklad.ru/api/remap/1.2")
    
    MOYSKLAD_RF_LOGIN = os.getenv("MOYSKLAD_RF_LOGIN") or MOYSKLAD_RU_LOGIN
    MOYSKLAD_RF_PASSWORD = os.getenv("MOYSKLAD_RF_PASSWORD") or MOYSKLAD_RU_PASSWORD
    MOYSKLAD_RF_BASE_URL = os.getenv("MOYSKLAD_RF_BASE_URL", MOYSKLAD_RU_BASE_URL or "https://api.moysklad.ru/api/remap/1.2")
    
    # МойСклад тестовые креденциалы
    MOYSKLAD_TEST_LOGIN = os.getenv("MOYSKLAD_TEST_LOGIN")
    MOYSKLAD_TEST_PASSWORD = os.getenv("MOYSKLAD_TEST_PASSWORD")
    MOYSKLAD_TEST_BASE_URL = os.getenv("MOYSKLAD_TEST_BASE_URL", "https://api.moysklad.ru/api/remap/1.2")
    
    # Битрикс24 настройки (общие для всех регионов)
    BITRIX24_WEBHOOK_URL = os.getenv("BITRIX24_WEBHOOK_URL")
    BITRIX24_CHAT_ID = os.getenv("BITRIX24_CHAT_ID")
    
    # Telegram бот настройки
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN")
    TELEGRAM_ALLOWED_USERS_RAW = os.getenv("TELEGRAM_ALLOWED_USERS", "")
    
    # Настройки мониторинга
    MIN_PRICE_THRESHOLD = float(os.getenv("MIN_PRICE_THRESHOLD", "0.01"))
    
    # Настройки API МойСклад
    MOYSKLAD_RATE_LIMIT = int(os.getenv("MOYSKLAD_RATE_LIMIT", "100"))  # запросов в минуту
    MOYSKLAD_DAILY_LIMIT = int(os.getenv("MOYSKLAD_DAILY_LIMIT", "1000"))  # запросов в день
    MOYSKLAD_MIN_DELAY = float(os.getenv("MOYSKLAD_MIN_DELAY", "0.1"))  # секунд между запросами
    
    # Настройки логирования
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @classmethod
    def get_moysklad_credentials(cls, region: str = None, use_test: bool = False) -> tuple:
        """
        Получение кредов МойСклад для региона
        
        Args:
            region: Регион (RB или RF)
            use_test: Использовать тестовые креденциалы
        
        Returns:
            tuple: (login, password, base_url)
        """
        if use_test:
            if cls.MOYSKLAD_TEST_LOGIN and cls.MOYSKLAD_TEST_PASSWORD:
                return cls.MOYSKLAD_TEST_LOGIN, cls.MOYSKLAD_TEST_PASSWORD, cls.MOYSKLAD_TEST_BASE_URL
            else:
                raise ValueError("Тестовые креденциалы МойСклад не настроены")
        
        if region is None:
            region = cls.REGION
        
        if region == "RB":
            return cls.MOYSKLAD_RB_LOGIN, cls.MOYSKLAD_RB_PASSWORD, cls.MOYSKLAD_RB_BASE_URL
        elif region == "RF":
            return cls.MOYSKLAD_RF_LOGIN, cls.MOYSKLAD_RF_PASSWORD, cls.MOYSKLAD_RF_BASE_URL
        elif region == "KZ":
            return cls.MOYSKLAD_KZ_LOGIN, cls.MOYSKLAD_KZ_PASSWORD, cls.MOYSKLAD_KZ_BASE_URL
        else:
            raise ValueError(f"Неподдерживаемый регион: {region}")
    
    @classmethod
    def get_telegram_bot_token(cls) -> str:
        """
        Получение токена Telegram бота
        
        Returns:
            str: Токен бота
        """
        if not cls.TELEGRAM_BOT_TOKEN:
            raise ValueError("Токен Telegram бота не настроен (TELEGRAM_BOT_TOKEN)")
        return cls.TELEGRAM_BOT_TOKEN

    @classmethod
    def get_telegram_allowed_users(cls) -> set:
        """Возвращает множество разрешённых user_id Telegram.

        Пустое множество означает отсутствие ограничения.
        """
        raw = cls.TELEGRAM_ALLOWED_USERS_RAW.strip()
        if not raw:
            return set()

        allowed = set()
        for token in raw.replace(";", ",").replace("\n", ",").split(","):
            token = token.strip()
            token = token.replace('"', '').replace("'", '')
            if not token:
                continue
            try:
                allowed.add(int(token))
            except ValueError as exc:
                raise ValueError(
                    f"Не удалось преобразовать значение '{token}' из TELEGRAM_ALLOWED_USERS в число"
                ) from exc

        return allowed
    
    @classmethod
    def validate(cls):
        """Проверка обязательных настроек"""
        region = cls.REGION
        
        # Проверяем креды МойСклад для текущего региона
        moysklad_login, moysklad_password, moysklad_url = cls.get_moysklad_credentials(region)
        if not moysklad_login or not moysklad_password:
            raise ValueError(f"Отсутствуют креды МойСклад для региона {region}")
        
        # Проверяем креды Битрикс24 (общие)
        if not cls.BITRIX24_WEBHOOK_URL or not cls.BITRIX24_CHAT_ID:
            raise ValueError("Отсутствуют креды Битрикс24")
        
        return True
