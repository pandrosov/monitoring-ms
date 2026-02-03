from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from loguru import logger
from moysklad_client import MoySkladClient
from bitrix24_client import Bitrix24Client
from config import Config

class MonitoringServiceV2:
    """Сервис мониторинга документов МойСклад с поддержкой регионов"""
    
    def __init__(self, region: str = None):
        self.region = (region or Config.REGION).upper()
        self.moysklad_client = MoySkladClient(self.region)
        self.bitrix24_client = Bitrix24Client()  # Общий для всех регионов
        self.min_price_threshold = Config.MIN_PRICE_THRESHOLD
        self.contact_center_employee = Config.CONTACT_CENTER_EMPLOYEE
        self._owner_cache: Dict[str, str] = {}
        
        logger.info(f"Инициализирован сервис мониторинга для региона {self.region}")
    
    def _build_document_link(self, document: Dict[str, Any], fallback_entity: str) -> str:
        """Формирование ссылки на документ в интерфейсе МойСклад"""
        if not isinstance(document, dict):
            return ""

        entity_type = fallback_entity
        doc_id = document.get("id")
        href = None

        meta = document.get("meta")
        if isinstance(meta, dict):
            href = meta.get("href")
            entity_type = meta.get("type") or entity_type
            if not doc_id and href:
                doc_id = href.rstrip("/").split("/")[-1]

        if not doc_id:
            return href or ""

        entity_key = (entity_type or fallback_entity or "").lower()
        entity_map = {
            "demand": "demand",
            "shipment": "demand",
            "salesreturn": "salesreturn",
            "retaildemand": "retaildemand",
            "commissionreportin": "commissionreportin",
            "commission": "commissionreportin",
            "counterparty": "Company",
            "contractor": "Company"
        }

        path = entity_map.get(entity_key, entity_map.get(fallback_entity.lower(), fallback_entity))
        if not path:
            path = fallback_entity.lower()
            if not path:
                return href or ""

        return f"https://online.moysklad.ru/app/#{path}/edit?id={doc_id}"

    def run_monitoring(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> bool:
        """
        Запуск мониторинга документов за указанный период
        
        Args:
            start_date: Дата начала периода (по умолчанию - вчера)
            end_date: Дата окончания периода (по умолчанию - вчера)
        """
        if start_date is None:
            start_date = date.today() - timedelta(days=1)
        if end_date is None:
            end_date = start_date
        
        logger.info(f"🚀 Запуск мониторинга за период {start_date} - {end_date} (регион: {self.region})")
        
        try:
            total_issues = 0
            
            # Проверяем контрагентов
            contractors_result = self.check_contractors_period(start_date, end_date)
            contractor_errors = contractors_result.get("errors", [])
            total_issues += len(contractor_errors)
            
            if contractor_errors:
                self.bitrix24_client.send_contractor_notification(contractor_errors)
            
            # Проверяем отгрузки
            shipments_result = self.check_shipments_period(start_date, end_date)
            shipment_errors = shipments_result.get("errors", [])
            total_issues += len(shipment_errors)
            
            if shipment_errors:
                self.bitrix24_client.send_shipment_notification(shipment_errors)
            
            # Проверяем отчеты комиссионеров
            commission_result = self.check_commission_reports_period(start_date, end_date)
            commission_errors = commission_result.get("errors", [])
            total_issues += len(commission_errors)
            
            if commission_errors:
                self.bitrix24_client.send_price_notification("Отчеты комиссионеров", commission_errors)
            
            # Проверяем продажи
            sales_result = self.check_sales_period(start_date, end_date)
            sales_errors = sales_result.get("errors", [])
            total_issues += len(sales_errors)
            
            if sales_errors:
                self.bitrix24_client.send_price_notification("Продажи", sales_errors)
            
            # Проверяем возвраты (только для РБ и РФ)
            if self.region in {"RB", "RF"}:
                # Возвраты покупателей
                sales_returns_result = self.check_sales_returns_period(start_date, end_date)
                sales_returns_errors = sales_returns_result.get("errors", [])
                total_issues += len(sales_returns_errors)
                
                if sales_returns_errors:
                    self.bitrix24_client.send_price_notification("Возвраты покупателей", sales_returns_errors)
                
                # Возвраты розницы
                retail_returns_result = self.check_retail_returns_period(start_date, end_date)
                retail_returns_errors = retail_returns_result.get("errors", [])
                total_issues += len(retail_returns_errors)
                
                if retail_returns_errors:
                    self.bitrix24_client.send_price_notification("Возвраты розницы", retail_returns_errors)
                
                # Возвраты комиссионеров
                commission_returns_result = self.check_commission_returns_period(start_date, end_date)
                commission_returns_errors = commission_returns_result.get("errors", [])
                total_issues += len(commission_returns_errors)
                
                if commission_returns_errors:
                    self.bitrix24_client.send_price_notification("Возвраты комиссионеров", commission_returns_errors)
            
            # Отправляем общий отчет
            if total_issues == 0:
                self.bitrix24_client.send_notification(
                    "Мониторинг МойСклад", 
                    f"✅ Проверка за период {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')} завершена. Проблем не обнаружено.",
                    "low"
                )
            else:
                self.bitrix24_client.send_notification(
                    "Мониторинг МойСклад", 
                    f"⚠️ Проверка за период {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')} завершена. Обнаружено {total_issues} проблем.",
                    "normal"
                )
            
            logger.info(f"✅ Мониторинг завершен. Найдено {total_issues} проблем")
            return True
            
        except Exception as e:
            error_msg = f"❌ Ошибка при выполнении мониторинга: {e}"
            logger.error(error_msg)
            self.bitrix24_client.send_notification("Ошибка мониторинга", error_msg, "high")
            return False
    
    def check_contractors_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка контрагентов за период"""
        logger.info(f"🔍 Проверка контрагентов за период {start_date} - {end_date}...")
        
        try:
            # Получаем контрагентов за период
            try:
                contractors = self.moysklad_client.get_contractors_for_period(start_date, end_date)
            except RuntimeError as e:
                if "лимит" in str(e).lower():
                    logger.error(f"❌ {str(e)}")
                    return {
                        "total": 0,
                        "valid": 0,
                        "errors": [],
                        "status": "error",
                        "error": str(e)
                    }
                raise
            
            if not contractors:
                logger.info("📋 Контрагентов за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📋 Найдено контрагентов: {len(contractors)}")
            
            errors = []
            valid_count = 0
            
            for contractor in contractors:
                contractor_name = contractor.get("name", "Без названия")
                contractor_id = contractor.get("id", "Без ID")
                company_type = (contractor.get("companyType") or "").lower()
                owner_name, owner_id = self._extract_contractor_owner(contractor)

                # Телефон: пытаемся получить из нескольких источников и валидируем
                phone_raw = self._extract_contractor_phone(contractor)
                phone_error = self._validate_phone(phone_raw)

                # Проверки для конкретных типов/регионов
                pd_agreement_error = ""
                pd_date_error = ""
                if self.region == "RB" and company_type == "individual":
                    pd_agreement_error = self._validate_pd_agreement(contractor)
                    pd_date_error = self._validate_pd_agreement_date(contractor)

                unp_error = self._validate_unp(contractor)
                type_name_mismatch_error = self._validate_type_name_consistency(contractor)
                actual_address_error = self._validate_actual_address(contractor)
                groups_error = self._validate_contractor_groups(contractor)
                
                # Проверки справочников (только для ЮЛ/ИП)
                contract_type_error = ""
                client_type_error = ""
                region_error = ""
                if company_type in {"legal", "entrepreneur"}:
                    contract_type_error = self._validate_contractor_contract_type(contractor)
                    client_type_error = self._validate_contractor_client_type(contractor)
                    region_error = self._validate_contractor_region(contractor)

                issues: List[str] = []
                if phone_error:
                    issues.append(f"Телефон: {phone_error}")
                if pd_agreement_error:
                    issues.append(f"Соглашение ПД: {pd_agreement_error}")
                if pd_date_error:
                    issues.append(f"Соглашение ПД (дата): {pd_date_error}")
                if unp_error:
                    issues.append(f"УНП/ИНН: {unp_error}")
                if actual_address_error:
                    issues.append(f"Фактический адрес: {actual_address_error}")
                if groups_error:
                    issues.append(f"Группа: {groups_error}")
                if type_name_mismatch_error:
                    issues.append(f"Тип ↔ Наименование: {type_name_mismatch_error}")
                if contract_type_error:
                    issues.append(f"Тип договора: {contract_type_error}")
                if client_type_error:
                    issues.append(f"Тип клиента: {client_type_error}")
                if region_error:
                    issues.append(f"Регион РБ: {region_error}")

                if issues:
                    error_info = {
                        "id": contractor_id,
                        "name": contractor_name,
                        "owner": owner_name,
                        "owner_id": owner_id,
                        "company_type": company_type,
                        "phone": phone_raw,
                        "phone_error": phone_error,
                        "pd_agreement_error": pd_agreement_error,
                        "pd_date_error": pd_date_error,
                        "unp_error": unp_error,
                        "actual_address_error": actual_address_error,
                        "groups_error": groups_error,
                        "type_name_mismatch_error": type_name_mismatch_error,
                        "issues": issues,
                        "link": self._build_document_link(contractor, "counterparty")
                    }
                    errors.append(error_info)
                    logger.warning(f"❌ Контрагент '{contractor_name}' имеет ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Контрагент '{contractor_name}' прошел все проверки")
            
            result = {
                "total": len(contractors),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка контрагентов завершена. Всего: {len(contractors)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки контрагентов: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def _resolve_owner(self, owner: Any) -> tuple[str, Optional[str]]:
        """Получает имя и идентификатор владельца, при необходимости запрашивает из API."""
        if not isinstance(owner, dict):
            return "Не указан", None

        name = owner.get("name")
        meta = owner.get("meta") or {}
        href = meta.get("href")

        owner_id: Optional[str] = None
        cache_key: Optional[str] = None

        if href:
            owner_id = href.rstrip("/").split("/")[-1]
            cache_key = owner_id or href

        if (not name or not str(name).strip()) and href:
            cached = self._owner_cache.get(cache_key or "")
            if cached:
                name = cached
            else:
                try:
                    data = self.moysklad_client._make_request(
                        href.replace(self.moysklad_client.base_url, "")
                    )
                    if data:
                        name = data.get("name") or data.get("fullName") or data.get("login")
                        if cache_key and name:
                            self._owner_cache[cache_key] = name
                except Exception as exc:
                    logger.warning(f"Не удалось получить данные владельца: {exc}")

        if not name or not str(name).strip():
            name = "Не указан"

        return str(name), owner_id

    def _extract_contractor_owner(self, contractor: Dict[str, Any]) -> tuple[str, Optional[str]]:
        """Возвращает имя и идентификатор владельца документа"""
        return self._resolve_owner(contractor.get("owner"))

    def _extract_contractor_phone(self, contractor: Dict[str, Any]) -> str:
        """Извлекает телефон контрагента из стандартных и дополнительных полей"""
        phone = contractor.get("phone")
        if phone:
            return str(phone).strip()

        # Ищем в дополнительных атрибутах
        attributes = contractor.get("attributes") or []
        for attribute in attributes:
            name = (attribute.get("name") or "").lower()
            if "тел" in name:
                value = attribute.get("value")
                if isinstance(value, str) and value.strip():
                    return value.strip()

        return ""

    def _get_counterparty_type(self, document: Dict[str, Any]) -> Optional[str]:
        """Возвращает тип контрагента (legal, entrepreneur, individual) с кэшированием"""
        cache_key = "_cached_company_type"
        if cache_key in document:
            return document[cache_key]

        agent = document.get("agent") or {}
        company_type = agent.get("companyType")

        if not company_type and isinstance(agent, dict):
            href = agent.get("meta", {}).get("href")
            if href:
                try:
                    agent_data = self.moysklad_client._make_request(
                        href.replace(self.moysklad_client.base_url, "")
                    )
                    if agent_data:
                        company_type = agent_data.get("companyType")
                except Exception as exc:
                    logger.warning(f"Не удалось загрузить тип контрагента: {exc}")

        if not company_type:
            # Попытка получить тип из атрибутов документа
            for attr in document.get("attributes", []) or []:
                name = str(attr.get("name", "")).lower()
                if "тип контрагента" in name or "companytype" in name:
                    val = attr.get("value")
                    if isinstance(val, str):
                        company_type = val
                    elif isinstance(val, dict):
                        company_type = val.get("name")
                    break

        company_type_normalized = company_type.lower() if isinstance(company_type, str) else None
        document[cache_key] = company_type_normalized
        return company_type_normalized

    def _validate_phone(self, phone: str) -> str:
        """Проверка номера телефона в зависимости от региона"""
        if not phone or not isinstance(phone, str):
            return "Телефон не указан"

        clean_phone = ''.join(char for char in phone if char.isdigit())
        if not clean_phone:
            return f"Телефон содержит недопустимые символы: {phone}"

        def _starts_with(prefixes: List[str]) -> bool:
            return any(clean_phone.startswith(pref) for pref in prefixes)

        if self.region == "RB":
            if not _starts_with(["375"]):
                return f"Номер должен начинаться с 375: {phone}"
            if len(clean_phone) != 12:
                return f"Неверная длина номера: {len(clean_phone)} цифр (должно быть 12)"
        elif self.region == "RF":
            if not _starts_with(["7", "8"]):
                return f"Номер должен начинаться с 7: {phone}"
            if len(clean_phone) != 11:
                return f"Неверная длина номера: {len(clean_phone)} цифр (должно быть 11)"
        elif self.region == "KZ":
            if not _starts_with(["7"]):
                return f"Номер должен начинаться с 7: {phone}"
            if len(clean_phone) != 11:
                return f"Неверная длина номера: {len(clean_phone)} цифр (должно быть 11)"
        else:
            if len(clean_phone) < 10:
                return f"Номер слишком короткий: {len(clean_phone)} цифр"
            if len(clean_phone) > 15:
                return f"Номер слишком длинный: {len(clean_phone)} цифр"
        
        return ""  # Нет ошибок
    
    def _validate_pd_agreement(self, contractor: Dict[str, Any]) -> str:
        """Проверка поля 'Соглашение политики ПД' (только для РБ)"""
        if self.region != "RB":
            return ""  # Проверяем только для РБ
        
        company_type = (contractor.get("companyType") or "").lower()
        
        # Проверяем только для физических лиц
        if company_type != "individual":
            return ""
        
        # Ищем поле "Соглашение политики ПД" в attributes
        attributes = contractor.get("attributes", [])
        
        allowed_values = {"принял согласие", "принял соглашение"}

        for attribute in attributes:
            attribute_name = attribute.get("name", "")
            if "Соглашение политики ПД" in attribute_name:
                attribute_value = attribute.get("value")
                
                # Проверяем, что значение равно "Принял согласие"
                if attribute_value:
                    if isinstance(attribute_value, dict):
                        value_name = attribute_value.get("name", "")
                    else:
                        value_name = str(attribute_value)

                    if value_name and value_name.strip().lower() in allowed_values:
                        return ""  # Нет ошибок
                    return (
                        f"Неверное значение: '{value_name}' "
                        "(должно быть 'Принял согласие' или 'Принял соглашение')"
                    )
                return "Поле не заполнено"
        
        return "Поле 'Соглашение политики ПД' не найдено"
    
    def _validate_pd_agreement_date(self, contractor: Dict[str, Any]) -> str:
        """Проверка поля 'Дата окончания соглашения ПД' (только для РБ)"""
        if self.region != "RB":
            return ""  # Проверяем только для РБ
        
        company_type = (contractor.get("companyType") or "").lower()
        
        # Проверяем только для физических лиц
        if company_type != "individual":
            return ""
        
        # Ищем поле "Дата окончания соглашения ПД" в attributes
        attributes = contractor.get("attributes", [])
        
        parse_formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
        ]

        for attribute in attributes:
            attribute_name = attribute.get("name", "")
            if "Дата окончания соглашения ПД" in attribute_name:
                attribute_value = attribute.get("value")
                
                if attribute_value:
                    try:
                        # Парсим дату
                        if isinstance(attribute_value, str):
                            agreement_date = None
                            for fmt in parse_formats:
                                try:
                                    agreement_date = datetime.strptime(attribute_value, fmt).date()
                                    break
                                except ValueError:
                                    continue
                            if agreement_date is None:
                                raise ValueError("unsupported format")
                        else:
                            agreement_date = attribute_value
                        
                        # Проверяем, что дата больше текущей даты на месяц
                        min_date = date.today() + timedelta(days=30)
                        if agreement_date < min_date:
                            return f"Дата окончания соглашения ПД ({agreement_date}) меньше чем через месяц от текущей даты"
                        
                        return ""  # Нет ошибок
                    except Exception as e:
                        return f"Неверный формат даты: {attribute_value}"
                else:
                    return "Поле не заполнено"
        
        return "Поле 'Дата окончания соглашения ПД' не найдено"
    
    def _validate_unp(self, contractor: Dict[str, Any]) -> str:
        """Проверка УНП/ИНН для юридических лиц и индивидуальных предпринимателей"""
        company_type = (contractor.get("companyType") or "").lower()
        
        # Проверяем только для юр. лиц и ИП
        if company_type not in ["legal", "entrepreneur"]:
            return ""  # Не проверяем для физ. лиц
        
        # Ищем УНП/ИНН в стандартных полях
        unp = (
            contractor.get("unp")
            or contractor.get("inn")
            or contractor.get("taxNumber")
        )
        
        # Если не нашли напрямую, проверяем реквизиты
        if not unp:
            requisites = contractor.get("requisites", {})
            if isinstance(requisites, dict):
                # УНП может быть в поле "unp", "inn" или "УНП"
                unp = (
                    requisites.get("unp")
                    or requisites.get("inn")
                    or requisites.get("УНП")
                )
        
        # Если не найден в requisites, проверяем поле code
        if not unp and contractor.get("code"):
            unp = contractor.get("code")
        
        # Если не найдено нигде выше, проверяем дополнительные атрибуты
        if not unp:
            attributes = contractor.get("attributes", [])
            for attribute in attributes:
                attribute_name = str(attribute.get("name", "")).lower()
                if any(token in attribute_name for token in ("унп", "инн", "идентификационный номер")):
                    value = attribute.get("value")
                    if isinstance(value, dict):
                        unp = value.get("name") or value.get("value")
                    else:
                        unp = value
                    if unp:
                        break
        
        if not unp:
            return "УНП/ИНН не заполнен"
        
        # Проверяем формат УНП/ИНН
        if isinstance(unp, str):
            unp_clean = ''.join(char for char in unp if char.isdigit())
            if self.region == "RB":
                # УНП для РБ: 9 цифр
                if len(unp_clean) != 9:
                    return f"Неверная длина УНП для РБ: {len(unp_clean)} цифр (должно быть 9)"
            elif self.region == "RF":
                # ИНН для РФ: 10 цифр для юр. лиц, 12 для ИП
                if company_type == "legal" and len(unp_clean) != 10:
                    return f"Неверная длина ИНН для юр. лица в РФ: {len(unp_clean)} цифр (должно быть 10)"
                elif company_type == "entrepreneur" and len(unp_clean) != 12:
                    return f"Неверная длина ИНН для ИП в РФ: {len(unp_clean)} цифр (должно быть 12)"
            
            if not unp_clean.isdigit():
                return f"УНП/ИНН содержит недопустимые символы: {unp}"
        else:
            return f"УНП/ИНН имеет неверный тип данных: {type(unp)}"
        
        return ""  # Нет ошибок

    def _validate_actual_address(self, contractor: Dict[str, Any]) -> str:
        """Проверка фактического адреса для юридических лиц"""
        company_type = (contractor.get("companyType") or "").lower()
        if company_type != "legal":
            return ""

        address = contractor.get("actualAddress")
        if isinstance(address, dict):
            address_str = address.get("fullAddress") or address.get("present") or ""
        else:
            address_str = address or ""

        if not isinstance(address_str, str) or not address_str.strip():
            return "Фактический адрес не заполнен"

        return ""

    def _validate_contractor_groups(self, contractor: Dict[str, Any]) -> str:
        """Проверка наличия групп/тегов для юридических лиц"""
        company_type = (contractor.get("companyType") or "").lower()
        if company_type != "legal":
            return ""

        tags = contractor.get("tags") or []
        if not tags:
            return "Группа (тег) не указана"

        return ""
    
    def _validate_type_name_consistency(self, contractor: Dict[str, Any]) -> str:
        """Проверка соответствия типа контрагента и наименования"""
        company_type = contractor.get("companyType", "")
        full_name = contractor.get("name", "").lower()
        
        if company_type == "legal":
            # Проверяем, не содержит ли наименование "Индивидуальный предприниматель"
            if "индивидуальный предприниматель" in full_name or "ип" in full_name:
                return f"Несоответствие: тип 'Юридическое лицо', но в наименовании указано 'Индивидуальный предприниматель'"
        elif company_type == "individual":
            # Проверяем, не содержит ли наименование "ООО" или "ОАО"
            if "ооо" in full_name or "оао" in full_name:
                return f"Несоответствие: тип 'Индивидуальный предприниматель', но в наименовании указано 'ООО/ОАО'"
        
        return ""  # Нет ошибок
    
    def _validate_contractor_contract_type(self, contractor: Dict[str, Any]) -> str:
        """Проверка заполненности справочника 'Тип договора' для контрагентов (РБ и РФ)"""
        if self.region not in {"RB", "RF"}:
            return ""
        
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        # Ищем поле "Тип договора" в атрибутах
        attributes = contractor.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            if _norm(attr_name) in {"типдоговора", "типдоговор"}:
                val = attr.get("value")
                if isinstance(val, dict):
                    value_name = val.get("name", "")
                    if value_name and str(value_name).strip():
                        return ""  # Заполнено
                elif isinstance(val, str) and val.strip():
                    return ""  # Заполнено
                return "Тип договора не заполнен"
        
        return "Тип договора не найден"
    
    def _validate_contractor_client_type(self, contractor: Dict[str, Any]) -> str:
        """Проверка заполненности справочника 'Тип клиента' для контрагентов (РБ и РФ)"""
        if self.region not in {"RB", "RF"}:
            return ""
        
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        # Ищем поле "Тип клиента" в атрибутах
        attributes = contractor.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            if _norm(attr_name) in {"типклиента", "типклиент"}:
                val = attr.get("value")
                if isinstance(val, dict):
                    value_name = val.get("name", "")
                    if value_name and str(value_name).strip():
                        return ""  # Заполнено
                elif isinstance(val, str) and val.strip():
                    return ""  # Заполнено
                return "Тип клиента не заполнен"
        
        return "Тип клиента не найден"
    
    def _validate_contractor_region(self, contractor: Dict[str, Any]) -> str:
        """Проверка заполненности справочника 'Регион РБ' для контрагентов (только РБ)"""
        if self.region != "RB":
            return ""
        
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        # Ищем поле "Регион РБ" в атрибутах
        attributes = contractor.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            if _norm(attr_name) in {"регионрб", "регион"}:
                val = attr.get("value")
                if isinstance(val, dict):
                    value_name = val.get("name", "")
                    if value_name and str(value_name).strip():
                        return ""  # Заполнено
                elif isinstance(val, str) and val.strip():
                    return ""  # Заполнено
                return "Регион РБ не заполнен"
        
        return "Регион РБ не найден"
    
    def check_shipments_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка отгрузок за период"""
        logger.info(f"🔍 Проверка отгрузок за период {start_date} - {end_date}...")
        
        try:
            # Получаем отгрузки за период
            try:
                shipments = self.moysklad_client.get_shipments_for_period(start_date, end_date)
            except RuntimeError as e:
                if "лимит" in str(e).lower():
                    logger.error(f"❌ {str(e)}")
                    return {
                        "total": 0,
                        "valid": 0,
                        "errors": [],
                        "status": "error",
                        "error": str(e)
                    }
                raise
            
            if not shipments:
                logger.info("📦 Отгрузок за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📦 Найдено отгрузок: {len(shipments)}")
            
            errors = []
            valid_count = 0
            
            for shipment in shipments:
                # Фильтр для KZ: исключаем отгрузки с "Kaspi" в комментариях
                if self.region == "KZ":
                    description = shipment.get("description", "") or ""
                    if "kaspi" in description.lower():
                        logger.debug(f"Пропускаем отгрузку '{shipment.get('name', '')}' - содержит 'Kaspi' в комментариях")
                        continue
                
                shipment_name = shipment.get("name", "Без названия")
                counterparty_name = (shipment.get("agent") or {}).get("name") or "Без контрагента"
                display_name = f"{shipment_name} ({counterparty_name})"
                shipment_id = shipment.get("id", "Без ID")
                
                # Получаем владельца
                owner = shipment.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                moment_dbg = shipment.get("moment", "")
                payed_sum_dbg = shipment.get("payedSum", 0) / 100
                total_sum_dbg = shipment.get("sum", 0) / 100
                
                # Проверяем владельца-сотрудника
                owner_error = self._validate_shipment_owner(shipment)
                
                # Проверяем источник продажи
                source_error = self._validate_sales_source(shipment)
                
                # Проверяем канал продаж
                channel_error = self._validate_sales_channel(shipment)
                
                # Проверяем проект для канала продаж
                project_error = self._validate_shipment_project(shipment)
                
                # Проверяем цены (только нулевые)
                price_errors = self._validate_shipment_prices(shipment)
                
                # Проверяем договор для юрлиц/ИП
                contract_error = self._validate_shipment_contract(shipment)
                
                # Проверяем поля договора (Тип договора и Скан)
                contract_fields_error = self._validate_contract_fields(shipment)
                
                # Проверяем тип договора для РФ (для ЮЛ/ИП)
                contract_type_shipment_error = self._validate_contract_type_shipment(shipment)
                
                # Проверяем метод расчета для юр. лиц и ИП
                payment_method_error = self._validate_payment_method(shipment)

                # Проверяем оплаты на основе условий договора
                payment_error = self._validate_shipment_payment(shipment)
                
                if (owner_error or source_error or channel_error or project_error or price_errors or 
                    payment_error or contract_error or contract_fields_error or payment_method_error or contract_type_shipment_error):
                    # Основные проверки
                    main_issues: List[str] = []
                    if owner_error:
                        main_issues.append(f"Владелец: {owner_error}")
                    if source_error:
                        main_issues.append(f"Источник продажи: {source_error}")
                    if channel_error:
                        main_issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        main_issues.append(f"Проект: {project_error}")
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            price_val = pe.get('price')
                            qty_val = pe.get('quantity')
                            details = f"Позиция '{product_name}': {issue_text}"
                            if price_val is not None:
                                details += f", цена={price_val}"
                            if qty_val is not None:
                                details += f", кол-во={qty_val}"
                            main_issues.append(details)
                    
                    # Проверки договоров
                    contract_issues: List[str] = []
                    if contract_error:
                        contract_issues.append(f"Договор: {contract_error}")
                    if contract_fields_error:
                        contract_issues.append(f"Поля договора: {contract_fields_error}")
                    if contract_type_shipment_error:
                        contract_issues.append(f"Тип договора: {contract_type_shipment_error}")
                    if payment_method_error:
                        contract_issues.append(f"Метод расчета: {payment_method_error}")
                    if payment_error:
                        contract_issues.append(f"Оплата: {payment_error}")
                    
                    # Общий список всех ошибок (для обратной совместимости)
                    issues: List[str] = main_issues + contract_issues

                    error_info = {
                        "id": shipment_id,
                        "name": shipment_name,
                        "display_name": display_name,
                        "counterparty": counterparty_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": moment_dbg,
                        "owner_error": owner_error,
                        "source_error": source_error,
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "contract_error": contract_error,
                        "contract_fields_error": contract_fields_error,
                        "contract_type_shipment_error": contract_type_shipment_error,
                        "payment_method_error": payment_method_error,
                        "price_errors": price_errors,
                        "payment_error": payment_error,
                        "main_issues": main_issues,
                        "contract_issues": contract_issues,
                        "issues": issues,
                        "link": self._build_document_link(shipment, "demand")
                    }
                    errors.append(error_info)
                    logger.warning("❌ Отгрузка '{}' ошибки: {}", display_name, "; ".join(issues))
                else:
                    valid_count += 1
                    logger.debug(f"✅ Отгрузка '{display_name}' прошла все проверки")
            
            result = {
                "total": len(shipments),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка отгрузок завершена. Всего: {len(shipments)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки отгрузок: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def _validate_shipment_owner(self, shipment: Dict[str, Any]) -> str:
        """Проверка владельца-сотрудника отгрузки"""
        owner = shipment.get("owner", {})
        owner_name = owner.get("name", "")
        
        if owner_name == self.contact_center_employee:
            return ""  # Владелец - Контакт-Центр, проверка пройдена
        else:
            return ""  # Не проверяем для других владельцев
    
    def _validate_sales_source(self, document: Dict[str, Any]) -> str:
        """Проверка источника продажи.
        
        Для отгрузок (demand) и отчетов комиссионеров (commissionreportin) поле требуется
        только если документ ведёт Контакт-центр. Для розничных продаж (retaildemand)
        требуем поле для Контакт-центра и для контрагентов-физлиц.
        """
        owner = document.get("owner", {})
        owner_name = owner.get("name", "")

        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())

        # Определяем, относится ли документ к Контакт-Центру
        norm_owner = _norm(owner_name)
        norm_cc = _norm(self.contact_center_employee)
        is_contact_center = norm_owner in {norm_cc, "контактцентр"}

        # Фоллбек: некоторые базы используют атрибут "Сотрудник: Контакт-центр"
        if not is_contact_center:
            for a in document.get("attributes", []) or []:
                name_norm = _norm(a.get("name", ""))
                if "сотрудник" in name_norm:
                    val = a.get("value")
                    val_name = (val or {}).get("name") if isinstance(val, dict) else (val if isinstance(val, str) else "")
                    if _norm(val_name) in {norm_cc, "контактцентр"}:
                        is_contact_center = True
                        break

        doc_type = ((document.get("meta") or {}).get("type") or "").lower()
        company_type = self._get_counterparty_type(document)
        is_physical = company_type == "individual"

        require_contact_center = True
        require_physical = False
        require_both = False  # Требовать и Контакт-Центр, и физлицо одновременно

        if doc_type in {"demand", "commissionreportin"}:
            # Для отгрузок и отчетов комиссионеров: проверяем только для физлиц при владельце Контакт Центр
            # Это работает для RB, RF и KZ
            require_both = True
        elif doc_type == "retaildemand":
            require_physical = True
        else:
            # Для неизвестных типов оставляем прежнее поведение.
            require_physical = True

        should_check = False
        if require_both:
            # Для отгрузок: требуем и Контакт-Центр, и физлицо
            if is_contact_center and is_physical:
                should_check = True
        elif require_contact_center and is_contact_center:
            should_check = True
        elif require_physical and is_physical:
            should_check = True

        if not should_check:
            return ""
        
        # Ищем поле "Источник продажи" в attributes
        attributes = document.get("attributes", [])
        
        # Совместимость с разными вариантами названий: ищем атрибут, в имени которого
        # встречаются токены "источник" и "продаж" (в любом числе/окончании)
        for attribute in attributes:
            attribute_name = attribute.get("name", "")
            name_norm = _norm(attribute_name)
            if ("источник" in name_norm) and ("продаж" in name_norm):
                attribute_value = attribute.get("value")
                # Значение-справочник: dict с name/meta
                if attribute_value:
                    if isinstance(attribute_value, dict):
                        value_name = attribute_value.get("name")
                        meta = attribute_value.get("meta") if isinstance(attribute_value, dict) else None
                        # Считаем заполненным, если есть meta.href или непустое имя
                        if isinstance(meta, dict) and meta.get("href"):
                            return ""
                        if value_name is not None and str(value_name).strip() != "":
                            return ""  # Ок
                        return "Поле 'Источник продажи' не заполнено"
                    # Строковое значение
                    if isinstance(attribute_value, str) and attribute_value.strip() != "":
                        return ""  # Ок
                return "Поле 'Источник продажи' не заполнено"
        
        return "Поле 'Источник продажи' не найдено"
    
    def _validate_sales_channel(self, shipment: Dict[str, Any]) -> str:
        """Проверка канала продаж (для всех отгрузок).
        В МойСклад это стандартное поле документа отгрузки: shipment['salesChannel'].
        Дополнительно пытаемся найти одноимённый кастомный атрибут, если стандартного поля нет.
        """
        # 1) Стандартное поле salesChannel
        sales_channel = shipment.get("salesChannel")
        if sales_channel is not None:
            # Обычно это объект-справочник: { meta, name }
            if isinstance(sales_channel, dict):
                # Если есть meta с href — считаем заполненным даже без name
                meta = sales_channel.get("meta")
                value_name = sales_channel.get("name")
                if (isinstance(meta, dict) and meta.get("href")) or (value_name is not None and str(value_name).strip() != ""):
                    return ""  # Ок
                # Иначе — незаполнено
                return "Поле 'Канал-продаж' не заполнено (salesChannel без meta/name)"
            # Если по какой-то причине пришло строковое значение
            if isinstance(sales_channel, str) and sales_channel.strip() != "":
                return ""  # Ок
            return "Поле 'Канал-продаж' не заполнено (salesChannel пустой)"

        # 2) Фоллбэк: поиск среди attributes (если в базе поле заведено как кастомное)
        attributes = shipment.get("attributes", [])
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        target_names = {"каналпродаж", "каналпродажи"}
        for attribute in attributes:
            attribute_name = attribute.get("name", "")
            if _norm(attribute_name) in target_names:
                attribute_value = attribute.get("value")
                if attribute_value:
                    if isinstance(attribute_value, dict):
                        value_name = attribute_value.get("name")
                        if value_name is not None and str(value_name).strip() != "":
                            return ""  # Ок
                        return "Поле 'Канал-продаж' не заполнено"
                    if isinstance(attribute_value, str) and attribute_value.strip() != "":
                        return ""  # Ок
                return "Поле 'Канал-продаж' не заполнено"

        return "Поле 'Канал-продаж' не найдено"
    
    def _validate_shipment_project(self, shipment: Dict[str, Any]) -> str:
        """Проверка соответствия проекта каналу продаж
        
        Правила на основе таблицы сопоставления:
        - Сети → должны быть проекты: Федеральные, Региональные, Локальные
        - Опт → должны быть проекты: Крупный Опт, Средний Опт, Салоны
        - Фарма → должен быть проект: Аптеки
        - Экспорт → должен быть проект: Экспорт Азия
        - Транзиты → должны быть проекты: Европа, ОАЭ, Казахстан, Беларусь, Россия
        - Остальные каналы (Маркетплейсы, Розница ИМ, Розница офлайн, Розница услуги, Розница сертификаты, CTM) → проект не требуется
        """
        def _norm(s: str) -> str:
            """Нормализация строки для сравнения"""
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        # Таблица сопоставления каналов и проектов
        CHANNEL_PROJECT_MAPPING = {
            "сети": ["федеральные", "региональные", "локальные"],
            "опт": ["крупныйопт", "среднийопт", "салоны"],
            "фарма": ["аптеки"],
            "экспорт": ["экспортазия"],
            "транзиты": ["европа", "оаэ", "казахстан", "беларусь", "россия"],
            # Каналы без обязательных проектов (пустой список означает, что проект не требуется)
            "маркетплейсы": [],
            "розницаим": [],
            "розницаофлайн": [],
            "розницауслуги": [],
            "розницасертификаты": [],
            "ctm": []
        }
        
        try:
            # Получаем канал продаж
            sales_channel_name = ""
            sales_channel = shipment.get("salesChannel")
            if isinstance(sales_channel, dict):
                sales_channel_name = sales_channel.get("name", "")
            elif isinstance(sales_channel, str):
                sales_channel_name = sales_channel
            
            # Если не нашли в стандартном поле, ищем в атрибутах
            if not sales_channel_name:
                attributes = shipment.get("attributes", [])
                for attr in attributes:
                    attr_name = attr.get("name", "")
                    if _norm(attr_name) in {"каналпродаж", "каналпродажи"}:
                        val = attr.get("value")
                        if isinstance(val, dict):
                            sales_channel_name = val.get("name", "")
                        elif isinstance(val, str):
                            sales_channel_name = val
                        break
            
            # Если канал продаж не найден, пропускаем проверку
            if not sales_channel_name:
                return ""
            
            channel_norm = _norm(sales_channel_name)
            
            # Получаем проект
            project = shipment.get("project")
            project_name = ""
            if isinstance(project, dict):
                project_name = project.get("name", "")
            elif isinstance(project, str):
                project_name = project
            
            project_norm = _norm(project_name) if project_name else ""
            
            # Ищем канал в таблице сопоставления
            allowed_projects = None
            for channel_key, projects in CHANNEL_PROJECT_MAPPING.items():
                if channel_key in channel_norm or channel_norm in channel_key:
                    allowed_projects = projects
                    break
            
            # Если канал не найден в таблице, пропускаем проверку
            if allowed_projects is None:
                return ""
            
            # Если для канала не требуется проект (пустой список), проверка пройдена
            if not allowed_projects:
                return ""
            
            # Для каналов с обязательными проектами проверяем соответствие
            if not project_name:
                # Находим ключ канала для формирования сообщения
                channel_key_found = None
                for key in CHANNEL_PROJECT_MAPPING.keys():
                    if key in channel_norm or channel_norm in key:
                        channel_key_found = key
                        break
                expected_list = CHANNEL_PROJECT_MAPPING.get(channel_key_found, allowed_projects) if channel_key_found else allowed_projects
                return f"Для канала '{sales_channel_name}' должен быть указан проект. Ожидается: {', '.join(expected_list)}"
            
            # Проверяем, что проект соответствует каналу
            allowed_projects_norm = {_norm(p) for p in allowed_projects}
            if project_norm not in allowed_projects_norm:
                # Находим ключ канала для формирования сообщения
                channel_key_found = None
                for key in CHANNEL_PROJECT_MAPPING.keys():
                    if key in channel_norm or channel_norm in key:
                        channel_key_found = key
                        break
                expected_projects = CHANNEL_PROJECT_MAPPING.get(channel_key_found, allowed_projects) if channel_key_found else allowed_projects
                return f"Для канала '{sales_channel_name}' указан некорректный проект '{project_name}'. Ожидается: {', '.join(expected_projects)}"
            
            return ""
            
        except Exception as e:
            logger.error(f"Ошибка проверки проекта: {e}")
            return f"Ошибка проверки проекта: {e}"

    def _validate_shipment_contract(self, shipment: Dict[str, Any]) -> str:
        """Проверка наличия договора для юрлиц и ИП.
        Требование: если контрагент companyType в {legal, individual} — должен быть заполнен contract (стандартное поле) либо явный атрибут договора.
        """
        try:
            company_type = self._get_counterparty_type(shipment)

            if not company_type:
                logger.debug("Тип контрагента неизвестен, пропускаем проверку договора")
                return ""

            # Требование справедливо только для юрлиц и ИП (entrepreneur)
            if company_type not in {"legal", "entrepreneur"}:
                logger.debug(f"Контрагент не юрлицо/ИП (тип: {company_type}), пропускаем проверку договора")
                return ""

            logger.debug(f"Проверяем договор для контрагента типа: {company_type}")

            # Проверяем стандартное поле contract
            contract = shipment.get("contract")
            if isinstance(contract, dict):
                meta = contract.get("meta")
                name = contract.get("name")
                if (isinstance(meta, dict) and meta.get("href")) or (name and str(name).strip() != ""):
                    logger.debug("Договор найден в стандартном поле")
                    return ""  # Ок — договор указан

            # Фоллбек: поищем среди атрибутов
            for a in shipment.get("attributes", []) or []:
                n = str(a.get("name", "")).lower()
                # Ищем именно "договор" или "contract", но исключаем "тип договора"
                if (n == "договор" or n == "contract") or (("договор" in n or "contract" in n) and "тип" not in n):
                    v = a.get("value")
                    if isinstance(v, dict):
                        vname = v.get("name")
                        vmeta = v.get("meta") if isinstance(v, dict) else None
                        if (isinstance(vmeta, dict) and vmeta.get("href")) or (vname and str(vname).strip() != ""):
                            logger.debug("Договор найден в атрибутах")
                            return ""
                    if isinstance(v, str) and v.strip() != "":
                        logger.debug("Договор найден в атрибутах (строка)")
                        return ""

            logger.debug("Договор не найден ни в стандартном поле, ни в атрибутах")
            return "Не указан договор для юрлица/ИП"
        except Exception as e:
            logger.error(f"Ошибка проверки договора в отгрузке: {e}")
            return "Ошибка проверки договора"
    
    def _validate_contract_fields(self, shipment: Dict[str, Any]) -> str:
        """Проверка обязательных полей договора: Тип договора и Скан договора"""
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        try:
            # Получаем договор
            contract = shipment.get("contract")
            if not contract or not isinstance(contract, dict):
                return ""  # Нет договора - не проверяем его поля
            
            contract_href = contract.get("meta", {}).get("href")
            if not contract_href:
                return ""
            
            try:
                # Запрашиваем полные данные договора
                contract_data = self.moysklad_client._make_request(
                    contract_href.replace(self.moysklad_client.base_url, "")
                )
                if not contract_data:
                    return ""
                
                errors = []
                
                # 1. Проверяем Тип договора (стандартное поле)
                contract_type = contract_data.get("contractType")
                if not contract_type:
                    errors.append("Не указан тип договора")
                
                # 2. Проверяем Скан договора (дополнительное поле типа файл)
                has_scan = False
                for attr in contract_data.get("attributes", []) or []:
                    attr_name = attr.get("name", "")
                    attr_type = attr.get("type", "")
                    if _norm(attr_name) in {"скандоговора", "сканд", "скан"}:
                        if attr_type == "file":
                            val = attr.get("value")
                            # Проверяем, что файл загружен (есть данные)
                            if val and (isinstance(val, dict) or isinstance(val, str)):
                                has_scan = True
                        break
                
                if not has_scan:
                    errors.append("Не загружен скан договора")
                
                if errors:
                    return "; ".join(errors)
                
                return ""
                
            except Exception as e:
                logger.warning(f"Не удалось проверить поля договора: {e}")
                return ""
        
        except Exception as e:
            logger.error(f"Ошибка проверки полей договора: {e}")
            return ""
    
    def _validate_contract_type_shipment(self, shipment: Dict[str, Any]) -> str:
        """Проверка заполненности типа договора в отгрузках для РФ (для ЮЛ/ИП)"""
        if self.region != "RF":
            return ""
        
        try:
            # Проверяем только для юрлиц и ИП
            company_type = self._get_counterparty_type(shipment)
            if company_type not in {"legal", "entrepreneur"}:
                return ""
            
            # Получаем договор
            contract = shipment.get("contract")
            if not contract or not isinstance(contract, dict):
                return ""  # Нет договора - не проверяем тип
            
            contract_href = contract.get("meta", {}).get("href")
            if not contract_href:
                return ""
            
            try:
                # Запрашиваем данные договора
                contract_data = self.moysklad_client._make_request(
                    contract_href.replace(self.moysklad_client.base_url, "")
                )
                if not contract_data:
                    return ""
                
                # Проверяем тип договора
                contract_type = contract_data.get("contractType")
                if not contract_type:
                    return "Тип договора не заполнен"
                
                return ""
                
            except Exception as e:
                logger.warning(f"Не удалось получить данные договора для проверки типа: {e}")
                return ""
            
        except Exception as e:
            logger.error(f"Ошибка проверки типа договора в отгрузке: {e}")
            return ""
    
    def _validate_payment_method(self, shipment: Dict[str, Any]) -> str:
        """Проверка метода расчета для юридических лиц и ИП
        
        Правила:
        - Для юр. лиц и ИП доступны только: р/с, р/с предоплата (школа-обучение, аренда)
        - Для этих методов обязателен договор и 100% оплата
        """
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        try:
            if self.region != "RB":
                return ""

            # Определяем тип контрагента
            company_type = self._get_counterparty_type(shipment)
            
            # Проверяем только для юр. лиц и ИП
            if company_type not in {"legal", "entrepreneur"}:
                return ""
            
            # Ищем "Метод расчета" в атрибутах отгрузки
            payment_method = None
            for attr in shipment.get("attributes", []) or []:
                attr_name = attr.get("name", "")
                if _norm(attr_name) in {"методрасчета", "методоплаты"}:
                    val = attr.get("value")
                    if isinstance(val, dict):
                        payment_method = val.get("name", "")
                    elif isinstance(val, str):
                        payment_method = val
                    break
            
            if not payment_method:
                return ""  # Если метод расчета не указан, не проверяем
            
            method_norm = _norm(payment_method)
            
            # Разрешенные методы для юр. лиц и ИП
            allowed_methods = {
                _norm("р/с"),
                _norm("р/с предоплата (школа-обучение, аренда)")
            }
            
            # Проверяем, что метод разрешен
            is_allowed = False
            for allowed in allowed_methods:
                if allowed in method_norm or method_norm in allowed:
                    is_allowed = True
                    break
            
            if not is_allowed:
                return f"Для юр. лиц/ИП недопустимый метод расчета: '{payment_method}'. Разрешены: р/с, р/с предоплата"
            
            # Для всех разрешенных методов проверяем наличие договора
            contract = shipment.get("contract")
            if not contract or not isinstance(contract, dict):
                return f"Метод расчета '{payment_method}' требует наличия договора"
            
            # Проверяем 100% оплату ТОЛЬКО для "р/с предоплата (школа-обучение, аренда)"
            # Для обычного "р/с" 100% оплата НЕ требуется
            if "предоплата" in method_norm and ("школа" in method_norm or "обучение" in method_norm or "аренда" in method_norm):
                total_sum = (shipment.get("sum", 0) or 0) / 100.0
                payed_sum = (shipment.get("payedSum", 0) or 0) / 100.0
                
                if total_sum > 0:
                    epsilon = 0.01
                    if payed_sum + epsilon < total_sum:
                        return f"Метод расчета '{payment_method}' требует 100% предоплаты. Оплачено: {payed_sum:.2f}, требуется: {total_sum:.2f}"
            
            return ""
        
        except Exception as e:
            logger.error(f"Ошибка проверки метода расчета: {e}")
            return ""
    
    def _validate_shipment_prices(self, shipment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Проверка цен в отгрузке - только нулевые цены"""
        price_errors = []
        
        try:
            positions = shipment.get("positions", {}).get("rows", [])
            
            for position in positions:
                product_name = position.get("assortment", {}).get("name", "Без названия")
                price = position.get("price", 0) / 100  # Цена в копейках
                quantity = position.get("quantity", 0)
                
                # Проверяем только нулевую цену
                if price == 0:
                    price_errors.append({
                        "product": product_name,
                        "issue": "Нулевая цена",
                        "price": price,
                        "quantity": quantity
                    })
            
        except Exception as e:
            logger.error(f"Ошибка проверки цен в отгрузке: {e}")
            price_errors.append({
                "product": "Ошибка проверки",
                "issue": f"Ошибка при проверке цен: {e}",
                "price": 0,
                "quantity": 0
            })
        
        return price_errors
    
    def _validate_shipment_payment(self, shipment: Dict[str, Any]) -> str:
        """Проверка оплаты отгрузки на основе условий договора
        
        Логика:
        1. Получаем договор из отгрузки
        2. Из договора получаем "Условие договора" (доп. поле типа справочник)
        3. Проверяем оплату в зависимости от условия:
           - Пропускаем: Без договора, предоставления безвозмездной (спонсорской) помощи, Договор комиссии
           - Проверяем 100% оплату: Предоплата, Реализация, Реализация Салоны
           - Проверяем отсрочку: Отсрочка 16-30 дней, Отсрочка 30-60 дней, Отсрочка 60 и более дней
        """
        def _norm(s: str) -> str:
            if not isinstance(s, str):
                return ""
            return "".join(ch for ch in s.lower() if ch.isalnum())
        
        try:
            if self.region not in {"RB", "RF"}:
                return ""

            # Получаем дату отгрузки
            moment_raw = shipment.get("moment")
            if not moment_raw:
                return ""  # Нет даты — пропускаем
            
            doc_dt: Optional[datetime] = None
            normalized = str(moment_raw).replace("Z", "").replace("T", " ")
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    doc_dt = datetime.strptime(normalized, fmt)
                    break
                except Exception:
                    pass
            if doc_dt is None:
                try:
                    doc_dt = datetime.fromisoformat(str(moment_raw).replace("Z", ""))
                except Exception:
                    return ""
            
            shipment_date = doc_dt.date()
            days_passed = (date.today() - shipment_date).days
            
            # Получаем суммы
            total_sum = (shipment.get("sum", 0) or 0) / 100.0
            payed_sum = (shipment.get("payedSum", 0) or 0) / 100.0
            
            if total_sum <= 0:
                return ""  # Нулевая сумма - не проверяем
            
            # Получаем договор
            contract = shipment.get("contract")
            if not contract or not isinstance(contract, dict):
                return ""  # Нет договора - не проверяем оплату
            
            contract_name = contract.get("name", "")
            
            # Получаем условие договора из API
            contract_href = contract.get("meta", {}).get("href")
            if not contract_href:
                return ""  # Нет ссылки на договор - не можем проверить условия
            
            try:
                # Запрашиваем данные договора
                contract_data = self.moysklad_client._make_request(
                    contract_href.replace(self.moysklad_client.base_url, "")
                )
                if not contract_data:
                    return ""
                
                # Ищем условие договора в атрибутах
                contract_condition = None
                for attr in contract_data.get("attributes", []) or []:
                    attr_name = attr.get("name", "")
                    if _norm(attr_name) in {"условиедоговора", "условие"}:
                        val = attr.get("value")
                        if isinstance(val, dict):
                            contract_condition = val.get("name", "")
                        elif isinstance(val, str):
                            contract_condition = val
                        break
                
                if not contract_condition:
                    return ""  # Нет условия договора - не проверяем
                
                condition_norm = _norm(contract_condition)
                
                # Проверяем, нужно ли пропустить проверку
                skip_conditions = {
                    _norm("Без договора"),
                    _norm("предоставления безвозмездной (спонсорской) помощи"),
                    _norm("Договор комиссии")
                }
                if condition_norm in skip_conditions:
                    return ""  # Эти условия не проверяем
                
                epsilon = 0.01  # Допуск на округление
                
                # Проверяем условия с обязательной 100% оплатой
                # Только Предоплата
                if condition_norm == _norm("Предоплата"):
                    if payed_sum + epsilon < total_sum:
                        return f"Условие договора '{contract_condition}': требуется 100% оплата. Оплачено: {payed_sum:.2f}, требуется: {total_sum:.2f}"
                    return ""
                
                # Проверяем отсрочки (проверяем наличие договора и оплату)
                if "отсрочка1630" in condition_norm or "отсрочка16-30" in condition_norm.replace(" ", ""):
                    # Проверяем договор всегда
                    if not contract or not isinstance(contract, dict):
                        return f"Условие 'Отсрочка 16-30 дней' требует наличия договора"
                    # Проверяем оплату только если прошло > 30 дней
                    if days_passed > 30 and payed_sum + epsilon < total_sum:
                        return f"Отсрочка 16-30 дней истекла (прошло {days_passed} дней). Оплачено: {payed_sum:.2f}, требуется: {total_sum:.2f}"
                    return ""
                
                if "отсрочка3060" in condition_norm or "отсрочка30-60" in condition_norm.replace(" ", ""):
                    # Проверяем договор всегда
                    if not contract or not isinstance(contract, dict):
                        return f"Условие 'Отсрочка 30-60 дней' требует наличия договора"
                    # Проверяем оплату только если прошло > 60 дней
                    if days_passed > 60 and payed_sum + epsilon < total_sum:
                        return f"Отсрочка 30-60 дней истекла (прошло {days_passed} дней). Оплачено: {payed_sum:.2f}, требуется: {total_sum:.2f}"
                    return ""
                
                if "отсрочка60" in condition_norm and "более" in condition_norm:
                    # Проверяем договор всегда
                    if not contract or not isinstance(contract, dict):
                        return f"Условие 'Отсрочка 60+ дней' требует наличия договора"
                    # Проверяем оплату только если прошло > 61 дня
                    if days_passed > 61 and payed_sum + epsilon < total_sum:
                        return f"Отсрочка 60+ дней истекла (прошло {days_passed} дней). Оплачено: {payed_sum:.2f}, требуется: {total_sum:.2f}"
                    return ""
                
                return ""  # Условие не распознано или не требует проверки
                
            except Exception as e:
                logger.warning(f"Не удалось получить данные договора: {e}")
                return ""
            
        except Exception as e:
            logger.error(f"Ошибка проверки оплаты отгрузки: {e}")
            return f"Ошибка проверки оплаты: {e}"
    
    def check_commission_reports_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка отчетов комиссионеров за период"""
        logger.info(f"🔍 Проверка отчетов комиссионеров за период {start_date} - {end_date}...")
        
        try:
            # Получаем отчеты за период
            reports = self.moysklad_client.get_commission_reports_for_period(start_date, end_date)
            
            if not reports:
                logger.info("📊 Отчетов комиссионеров за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📊 Найдено отчетов комиссионеров: {len(reports)}")
            
            errors = []
            valid_count = 0

            for report in reports:
                report_name = report.get("name", "Без названия")
                report_id = report.get("id", "Без ID")
                
                # Получаем владельца документа
                owner = report.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                price_errors = self._validate_commission_prices(report)
                channel_error = self._validate_sales_channel(report)
                project_error = self._validate_shipment_project(report)
                contract_error = self._validate_shipment_contract(report)
                contract_fields_error = self._validate_contract_fields(report) if not contract_error else ""
                source_error = self._validate_sales_source(report)
                payment_method_error = self._validate_payment_method(report)
                payment_error = self._validate_shipment_payment(report)
                
                if (
                    price_errors
                    or channel_error
                    or project_error
                    or contract_error
                    or contract_fields_error
                    or source_error
                    or payment_method_error
                    or payment_error
                ):
                    issues: List[str] = []
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            issues.append(f"Позиция '{product_name}': {issue_text}")
                    if channel_error:
                        issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        issues.append(f"Проект: {project_error}")
                    if contract_error:
                        issues.append(f"Договор: {contract_error}")
                    if contract_fields_error:
                        issues.append(f"Поля договора: {contract_fields_error}")
                    if source_error:
                        issues.append(f"Источник продажи: {source_error}")
                    if payment_method_error:
                        issues.append(f"Метод расчета: {payment_method_error}")
                    if payment_error:
                        issues.append(f"Оплата: {payment_error}")

                    error_info = {
                        "id": report_id,
                        "name": report_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": report.get("moment", ""),
                        "price_errors": price_errors,
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "contract_error": contract_error,
                        "contract_fields_error": contract_fields_error,
                        "source_error": source_error,
                        "payment_method_error": payment_method_error,
                        "payment_error": payment_error,
                        "issues": issues,
                        "link": self._build_document_link(report, "commissionreportin")
                    }
                    errors.append(error_info)
                    
                    logger.warning(f"❌ Отчет комиссионера '{report_name}' ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Отчет комиссионера '{report_name}' прошел все проверки")
            
            result = {
                "total": len(reports),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка отчетов комиссионеров завершена. Всего: {len(reports)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки отчетов комиссионеров: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def check_sales_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка продаж за период"""
        logger.info(f"🔍 Проверка продаж за период {start_date} - {end_date}...")
        
        try:
            # Получаем продажи за период
            sales = self.moysklad_client.get_sales_for_period(start_date, end_date)
            
            if not sales:
                logger.info("💰 Продаж за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"💰 Найдено продаж: {len(sales)}")
            
            errors = []
            valid_count = 0

            for sale in sales:
                sale_name = sale.get("name", "Без названия")
                sale_id = sale.get("id", "Без ID")
                
                # Получаем владельца документа
                owner = sale.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                price_errors = self._validate_sale_prices(sale)
                channel_error = self._validate_sales_channel(sale)
                project_error = self._validate_shipment_project(sale)
                contract_error = self._validate_shipment_contract(sale)
                contract_fields_error = self._validate_contract_fields(sale) if not contract_error else ""
                source_error = self._validate_sales_source(sale)
                payment_method_error = self._validate_payment_method(sale)
                payment_error = self._validate_shipment_payment(sale)
                
                if (
                    price_errors
                    or channel_error
                    or project_error
                    or contract_error
                    or contract_fields_error
                    or source_error
                    or payment_method_error
                    or payment_error
                ):
                    issues: List[str] = []
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            issues.append(f"Позиция '{product_name}': {issue_text}")
                    if channel_error:
                        issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        issues.append(f"Проект: {project_error}")
                    if contract_error:
                        issues.append(f"Договор: {contract_error}")
                    if contract_fields_error:
                        issues.append(f"Поля договора: {contract_fields_error}")
                    if source_error:
                        issues.append(f"Источник продажи: {source_error}")
                    if payment_method_error:
                        issues.append(f"Метод расчета: {payment_method_error}")
                    if payment_error:
                        issues.append(f"Оплата: {payment_error}")

                    error_info = {
                        "id": sale_id,
                        "name": sale_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": sale.get("moment", ""),
                        "price_errors": price_errors,
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "contract_error": contract_error,
                        "contract_fields_error": contract_fields_error,
                        "source_error": source_error,
                        "payment_method_error": payment_method_error,
                        "payment_error": payment_error,
                        "issues": issues,
                        "link": self._build_document_link(sale, "retaildemand")
                    }
                    errors.append(error_info)
                    
                    logger.warning(f"❌ Продажа '{sale_name}' ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Продажа '{sale_name}' прошла все проверки")
            
            result = {
                "total": len(sales),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка продаж завершена. Всего: {len(sales)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки продаж: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def check_sales_returns_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка возвратов покупателей за период"""
        logger.info(f"🔍 Проверка возвратов покупателей за период {start_date} - {end_date}...")
        
        try:
            returns = self.moysklad_client.get_sales_returns_for_period(start_date, end_date)
            
            if not returns:
                logger.info("📦 Возвратов покупателей за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📦 Найдено возвратов покупателей: {len(returns)}")
            
            errors = []
            valid_count = 0
            
            for return_doc in returns:
                return_name = return_doc.get("name", "Без названия")
                counterparty_name = (return_doc.get("agent") or {}).get("name") or "Без контрагента"
                display_name = f"{return_name} ({counterparty_name})"
                return_id = return_doc.get("id", "Без ID")
                
                owner = return_doc.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                # Проверяем канал продаж
                channel_error = self._validate_sales_channel(return_doc)
                
                # Проверяем проект для канала продаж
                project_error = self._validate_shipment_project(return_doc)
                
                # Проверяем цены (только нулевые)
                price_errors = self._validate_shipment_prices(return_doc)
                
                if channel_error or project_error or price_errors:
                    issues: List[str] = []
                    if channel_error:
                        issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        issues.append(f"Проект: {project_error}")
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            price_val = pe.get('price')
                            qty_val = pe.get('quantity')
                            details = f"Позиция '{product_name}': {issue_text}"
                            if price_val is not None:
                                details += f", цена={price_val}"
                            if qty_val is not None:
                                details += f", кол-во={qty_val}"
                            issues.append(details)
                    
                    error_info = {
                        "id": return_id,
                        "name": return_name,
                        "display_name": display_name,
                        "counterparty": counterparty_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": return_doc.get("moment", ""),
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "price_errors": price_errors,
                        "issues": issues,
                        "link": self._build_document_link(return_doc, "salesreturn")
                    }
                    errors.append(error_info)
                    logger.warning(f"❌ Возврат покупателя '{display_name}' ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Возврат покупателя '{display_name}' прошел все проверки")
            
            result = {
                "total": len(returns),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка возвратов покупателей завершена. Всего: {len(returns)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки возвратов покупателей: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def check_retail_returns_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка возвратов розницы за период"""
        logger.info(f"🔍 Проверка возвратов розницы за период {start_date} - {end_date}...")
        
        try:
            returns = self.moysklad_client.get_retail_returns_for_period(start_date, end_date)
            
            if not returns:
                logger.info("📦 Возвратов розницы за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📦 Найдено возвратов розницы: {len(returns)}")
            
            errors = []
            valid_count = 0
            
            for return_doc in returns:
                return_name = return_doc.get("name", "Без названия")
                counterparty_name = (return_doc.get("agent") or {}).get("name") or "Без контрагента"
                display_name = f"{return_name} ({counterparty_name})"
                return_id = return_doc.get("id", "Без ID")
                
                owner = return_doc.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                # Проверяем канал продаж
                channel_error = self._validate_sales_channel(return_doc)
                
                # Проверяем проект для канала продаж
                project_error = self._validate_shipment_project(return_doc)
                
                # Проверяем цены (только нулевые)
                price_errors = self._validate_shipment_prices(return_doc)
                
                if channel_error or project_error or price_errors:
                    issues: List[str] = []
                    if channel_error:
                        issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        issues.append(f"Проект: {project_error}")
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            price_val = pe.get('price')
                            qty_val = pe.get('quantity')
                            details = f"Позиция '{product_name}': {issue_text}"
                            if price_val is not None:
                                details += f", цена={price_val}"
                            if qty_val is not None:
                                details += f", кол-во={qty_val}"
                            issues.append(details)
                    
                    error_info = {
                        "id": return_id,
                        "name": return_name,
                        "display_name": display_name,
                        "counterparty": counterparty_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": return_doc.get("moment", ""),
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "price_errors": price_errors,
                        "issues": issues,
                        "link": self._build_document_link(return_doc, "retailsalesreturn")
                    }
                    errors.append(error_info)
                    logger.warning(f"❌ Возврат розницы '{display_name}' ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Возврат розницы '{display_name}' прошел все проверки")
            
            result = {
                "total": len(returns),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка возвратов розницы завершена. Всего: {len(returns)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки возвратов розницы: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def check_commission_returns_period(self, start_date: date, end_date: date) -> Dict[str, Any]:
        """Проверка возвратов комиссионеров за период"""
        logger.info(f"🔍 Проверка возвратов комиссионеров за период {start_date} - {end_date}...")
        
        try:
            returns = self.moysklad_client.get_commission_returns_for_period(start_date, end_date)
            
            if not returns:
                logger.info("📦 Возвратов комиссионеров за период не найдено")
                return {
                    "total": 0,
                    "valid": 0,
                    "errors": [],
                    "status": "success"
                }
            
            logger.info(f"📦 Найдено возвратов комиссионеров: {len(returns)}")
            
            errors = []
            valid_count = 0
            
            for return_doc in returns:
                return_name = return_doc.get("name", "Без названия")
                counterparty_name = (return_doc.get("agent") or {}).get("name") or "Без контрагента"
                display_name = f"{return_name} ({counterparty_name})"
                return_id = return_doc.get("id", "Без ID")
                
                owner = return_doc.get("owner", {})
                owner_name, owner_id = self._resolve_owner(owner)
                display_owner = owner_name
                
                # Проверяем канал продаж
                channel_error = self._validate_sales_channel(return_doc)
                
                # Проверяем проект для канала продаж
                project_error = self._validate_shipment_project(return_doc)
                
                # Проверяем цены (только нулевые)
                price_errors = self._validate_shipment_prices(return_doc)
                
                if channel_error or project_error or price_errors:
                    issues: List[str] = []
                    if channel_error:
                        issues.append(f"Канал продаж: {channel_error}")
                    if project_error:
                        issues.append(f"Проект: {project_error}")
                    if price_errors:
                        for pe in price_errors:
                            product_name = pe.get('product', 'Неизвестный товар')
                            issue_text = pe.get('issue', 'Проблема с ценой')
                            price_val = pe.get('price')
                            qty_val = pe.get('quantity')
                            details = f"Позиция '{product_name}': {issue_text}"
                            if price_val is not None:
                                details += f", цена={price_val}"
                            if qty_val is not None:
                                details += f", кол-во={qty_val}"
                            issues.append(details)
                    
                    error_info = {
                        "id": return_id,
                        "name": return_name,
                        "display_name": display_name,
                        "counterparty": counterparty_name,
                        "owner": display_owner,
                        "owner_id": owner_id,
                        "moment": return_doc.get("moment", ""),
                        "channel_error": channel_error,
                        "project_error": project_error,
                        "price_errors": price_errors,
                        "issues": issues,
                        "link": self._build_document_link(return_doc, "commissionreportout")
                    }
                    errors.append(error_info)
                    logger.warning(f"❌ Возврат комиссионера '{display_name}' ошибки: {'; '.join(issues)}")
                else:
                    valid_count += 1
                    logger.debug(f"✅ Возврат комиссионера '{display_name}' прошел все проверки")
            
            result = {
                "total": len(returns),
                "valid": valid_count,
                "errors": errors,
                "status": "success"
            }
            
            logger.info(f"✅ Проверка возвратов комиссионеров завершена. Всего: {len(returns)}, Валидных: {valid_count}, Ошибок: {len(errors)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки возвратов комиссионеров: {e}")
            return {
                "total": 0,
                "valid": 0,
                "errors": [],
                "status": "error",
                "error_message": str(e)
            }
    
    def _validate_sale_prices(self, sale: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Проверка цен в продаже - только нулевые цены (как в отгрузках)"""
        price_errors = []
        
        try:
            positions = sale.get("positions", {}).get("rows", [])
            
            for position in positions:
                product_name = position.get("assortment", {}).get("name", "Без названия")
                price = position.get("price", 0) / 100  # Цена в копейках
                quantity = position.get("quantity", 0)
                
                # Проверяем только нулевую цену
                if price == 0:
                    price_errors.append({
                        "product": product_name,
                        "issue": "Нулевая цена",
                        "price": price,
                        "quantity": quantity
                    })
            
        except Exception as e:
            logger.error(f"Ошибка проверки цен в продаже: {e}")
            price_errors.append({
                "product": "Ошибка проверки",
                "issue": f"Ошибка при проверке цен: {e}",
                "price": 0,
                "quantity": 0
            })
        
        return price_errors
    
    def _validate_commission_prices(self, report: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Проверка цен в отчете комиссионера - только нулевые цены"""
        price_errors = []
        
        try:
            positions = report.get("positions", {}).get("rows", [])
            
            for position in positions:
                product_name = position.get("assortment", {}).get("name", "Без названия")
                price = position.get("price", 0) / 100  # Цена в копейках
                quantity = position.get("quantity", 0)
                
                # Проверяем только нулевую цену
                if price == 0:
                    price_errors.append({
                        "product": product_name,
                        "issue": "Нулевая цена",
                        "price": price,
                        "quantity": quantity
                    })
            
        except Exception as e:
            logger.error(f"Ошибка проверки цен в отчете комиссионера: {e}")
            price_errors.append({
                "product": "Ошибка проверки",
                "issue": f"Ошибка при проверке цен: {e}",
                "price": 0,
                "quantity": 0
            })
        
        return price_errors
    
    def _validate_document_prices(self, document: Dict[str, Any], document_type: str, min_prices: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
        """Проверка цен в документе (для отчетов комиссионеров - с минимальными ценами)"""
        price_errors = []
        
        try:
            # Используем переданный кэш минимальных цен (если нет — пустой)
            if min_prices is None:
                min_prices = {}

            positions = document.get("positions", {}).get("rows", [])
            
            for position in positions:
                product_name = position.get("assortment", {}).get("name", "Без названия")
                price = position.get("price", 0) / 100  # Цена в копейках
                quantity = position.get("quantity", 0)
                
                # Проверяем нулевую цену
                if price == 0:
                    price_errors.append({
                        "product": product_name,
                        "issue": "Нулевая цена",
                        "price": price,
                        "quantity": quantity
                    })
                    continue
                
                # Проверяем цену ниже минимальной
                product_id = position.get("assortment", {}).get("id")
                if product_id in min_prices:
                    min_price = min_prices[product_id]
                    if price < min_price:
                        price_errors.append({
                            "product": product_name,
                            "issue": f"Цена ниже минимальной ({min_price})",
                            "price": price,
                            "min_price": min_price,
                            "quantity": quantity
                        })
            
        except Exception as e:
            logger.error(f"Ошибка проверки цен в {document_type.lower()}: {e}")
            price_errors.append({
                "product": "Ошибка проверки",
                "issue": f"Ошибка получения минимальных цен: {e}",
                "price": 0,
                "quantity": 0
            })
        
        return price_errors
