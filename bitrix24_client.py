import os
from pathlib import Path

import requests
from typing import Dict, Any, List, Optional
from loguru import logger
from config import Config

class Bitrix24Client:
    """Клиент для работы с API Битрикс24"""
    
    def __init__(self):
        self.webhook_url = Config.BITRIX24_WEBHOOK_URL
        self.chat_id = Config.BITRIX24_CHAT_ID
    
    def send_message_to_chat(self, message: str) -> bool:
        """Отправка сообщения в чат Битрикс24"""
        try:
            # Используем метод im.message.add для отправки сообщения в чат
            method = "im.message.add"
            
            # Формируем DIALOG_ID в зависимости от типа чата
            # Если chat_id уже содержит "chat", используем как есть
            # Иначе добавляем префикс "chat"
            if str(self.chat_id).startswith("chat"):
                dialog_id = str(self.chat_id)
            else:
                dialog_id = f"chat{self.chat_id}"
            
            # Формируем данные для отправки
            data = {
                "DIALOG_ID": dialog_id,
                "MESSAGE": message
            }
            
            url = f"{self.webhook_url}/{method}"
            
            headers = {
                "Content-Type": "application/json"
            }
            
            logger.debug(f"Отправка сообщения в чат: {dialog_id}")
            logger.debug(f"URL: {url}")
            logger.debug(f"Данные: {data}")
            
            # Отправляем данные как JSON в теле запроса
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("result"):
                    logger.info("Сообщение успешно отправлено в чат Битрикс24")
                    return True
                else:
                    logger.error(f"Ошибка отправки сообщения: {result.get('error_description', 'Неизвестная ошибка')}")
                    return False
            else:
                logger.error(f"HTTP ошибка при отправке сообщения: {response.status_code}")
                logger.error(f"Ответ сервера: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения в Битрикс24: {e}")
            return False

    def send_file_to_chat(self, file_path: os.PathLike[str] | str, caption: Optional[str] = None) -> bool:
        """Загрузка файла в Bitrix24 и отправка его в чат"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                logger.error(f"Файл для отправки в Bitrix24 не найден: {file_path}")
                return False

            upload_method = "disk.folder.uploadfile"
            upload_url = f"{self.webhook_url}/{upload_method}"
            files = {"file": open(file_path, "rb")}
            data = {
                "id": 0,  # корневой раздел пользователя
                "generateUniqueName": "Y",
                "data[fileName]": file_path.name
            }

            response = requests.post(upload_url, data=data, files=files)
            files["file"].close()

            if response.status_code != 200:
                logger.error(f"HTTP ошибка загрузки файла в Bitrix24: {response.status_code}")
                logger.error(f"Ответ сервера: {response.text}")
                return False

            result = response.json().get("result")
            if not result:
                logger.error(f"Не удалось получить результат загрузки файла: {response.text}")
                return False

            file_id = result.get("ID") or result.get("id") or result.get("FILE_ID")
            if not file_id:
                file_info = result.get("file") or {}
                file_id = file_info.get("ID") or file_info.get("id")

            if not file_id:
                logger.error(f"Не удалось определить ID загруженного файла: {response.text}")
                return False

            caption_text = caption or "📎 Полный список ошибок"
            file_message = f"{caption_text} [DISK={file_id}]"
            return self.send_message_to_chat(file_message)

        except Exception as e:
            logger.error(f"Ошибка отправки файла в Битрикс24: {e}")
            return False
    
    def send_shipment_errors_summary(self, errors: List[Dict[str, Any]], start_date, end_date, region: str) -> bool:
        """Отправка сводки по ошибкам в отгрузках за период"""
        if not errors:
            return True
        
        # Формируем заголовок
        header = f"📊 Мониторинг отгрузок {region} за {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n\n"
        header += f"Всего ошибок: {len(errors)}\n\n"
        
        # Группируем ошибки по типам
        contract_errors = []
        payment_errors = []
        source_errors = []
        channel_errors = []
        price_errors = []
        
        for error in errors:
            if error.get('contract_error'):
                contract_errors.append(error)
            if error.get('payment_error'):
                payment_errors.append(error)
            if error.get('source_error'):
                source_errors.append(error)
            if error.get('channel_error'):
                channel_errors.append(error)
            if error.get('price_errors'):
                price_errors.append(error)
        
        # Формируем сообщение
        message = header
        
        if contract_errors:
            message += f"📝 Не указан договор ({len(contract_errors)}):\n"
            for err in contract_errors[:10]:  # Показываем первые 10
                message += f"  • {err['name']}\n"
            if len(contract_errors) > 10:
                message += f"  ... и еще {len(contract_errors) - 10}\n"
            message += "\n"
        
        if payment_errors:
            message += f"💳 Недостаточная оплата ({len(payment_errors)}):\n"
            for err in payment_errors[:10]:  # Показываем первые 10
                message += f"  • {err['name']}\n"
            if len(payment_errors) > 10:
                message += f"  ... и еще {len(payment_errors) - 10}\n"
            message += "\n"
        
        if source_errors:
            message += f"📊 Не указан источник продажи ({len(source_errors)}):\n"
            for err in source_errors[:10]:
                message += f"  • {err['name']}\n"
            if len(source_errors) > 10:
                message += f"  ... и еще {len(source_errors) - 10}\n"
            message += "\n"
        
        if channel_errors:
            message += f"📺 Не указан канал продаж ({len(channel_errors)}):\n"
            for err in channel_errors[:10]:
                message += f"  • {err['name']}\n"
            if len(channel_errors) > 10:
                message += f"  ... и еще {len(channel_errors) - 10}\n"
            message += "\n"
        
        if price_errors:
            message += f"💰 Проблемы с ценами ({len(price_errors)}):\n"
            for err in price_errors[:10]:
                message += f"  • {err['name']}\n"
            if len(price_errors) > 10:
                message += f"  ... и еще {len(price_errors) - 10}\n"
            message += "\n"
        
        return self.send_message_to_chat(message)
