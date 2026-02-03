"""
Telegram бот для мониторинга документов МойСклад
"""
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters
)
from loguru import logger
from openpyxl import Workbook

from monitoring_service_v2 import MonitoringServiceV2
from config import Config

# Стадии разговора
REGION, DOCUMENT, DATE_FROM, DATE_TO, BITRIX = range(5)

# Данные для выбора
REGIONS = {
    'rb': '🇧🇾 Беларусь (РБ)',
    'rf': '🇷🇺 Россия (РФ)',
    'kz': '🇰🇿 Казахстан (КЗ)'
}

DOCUMENTS = {
    'shipments': '📦 Отгрузки',
    'sales': '💰 Продажи',
    'commission': '📋 Отчеты комиссионеров',
    'contractors': '👥 Контрагенты'
}

# Ограничения и настройки
MAX_MESSAGE_LENGTH = 3500
MAX_DOCUMENTS_PER_OWNER: int | None = None
REPORTS_DIR = Path("reports")

# Хранилище данных пользователя
user_data_storage: Dict[int, Dict[str, Any]] = {}


class TelegramMonitoringBot:
    """Telegram бот для мониторинга МойСклад"""
    
    def __init__(self):
        """Инициализация бота"""
        self.config = Config()
        self.token = self.config.get_telegram_bot_token()
        self.allowed_users = self.config.get_telegram_allowed_users()
        self.services = {}  # Кэш сервисов по регионам
        
        # Настройка логирования
        logger.add(
            "logs/telegram_bot.log",
            rotation="1 day",
            retention="7 days",
            level="INFO"
        )
    
    def get_service(self, region: str) -> MonitoringServiceV2:
        """Получить сервис мониторинга для региона"""
        if region not in self.services:
            self.services[region] = MonitoringServiceV2(region=region.upper())
        return self.services[region]

    def _is_user_allowed(self, user_id: int) -> bool:
        """Проверка доступа пользователя к боту."""
        if not self.allowed_users:
            return True
        return user_id in self.allowed_users

    @staticmethod
    def _create_period_keyboard() -> InlineKeyboardMarkup:
        """Клавиатура выбора периода"""
        keyboard = [
            [InlineKeyboardButton('📅 Сегодня', callback_data='period_today')],
            [InlineKeyboardButton('📅 Вчера', callback_data='period_yesterday')],
            [InlineKeyboardButton('📅 Последние 3 дня', callback_data='period_3days')],
            [InlineKeyboardButton('📅 Последняя неделя', callback_data='period_week')],
            [InlineKeyboardButton('📅 Последний месяц', callback_data='period_month')],
            [InlineKeyboardButton('✏️ Указать период вручную', callback_data='period_custom')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='back_to_document')]
        ]
        return InlineKeyboardMarkup(keyboard)

    @staticmethod
    def _group_errors_by_owner(errors: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for error in errors or []:
            owner = error.get('owner') or 'Не указан'
            grouped[owner].append(error)
        # Сортируем по количеству ошибок (desc), затем по имени владельца
        return dict(sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0] or "")))

    @staticmethod
    def _extract_issues(error: Dict[str, Any]) -> List[str]:
        predefined = error.get('issues')
        if isinstance(predefined, list) and predefined:
            return predefined

        labels = {
            'owner_error': 'Владелец',
            'source_error': 'Источник продажи',
            'channel_error': 'Канал продаж',
            'project_error': 'Проект',
            'contract_error': 'Договор',
            'contract_fields_error': 'Поля договора',
            'payment_method_error': 'Метод расчета',
            'payment_error': 'Оплата',
            'price_error': 'Цена',
            'phone_error': 'Телефон',
            'pd_agreement_error': 'Соглашение ПД',
            'pd_date_error': 'Дата соглашения ПД',
            'unp_error': 'УНП/ИНН',
            'actual_address_error': 'Фактический адрес',
            'groups_error': 'Группа контрагентов',
            'type_name_mismatch_error': 'Тип ↔ Наименование'
        }

        issues: List[str] = []

        for key, value in error.items():
            if key.endswith('_error') and value:
                label = labels.get(key, key.replace('_', ' ').capitalize())
                issues.append(f"{label}: {value}")

        price_errors = error.get('price_errors') or []
        if isinstance(price_errors, list):
            for pe in price_errors:
                if not isinstance(pe, dict):
                    continue
                product_name = pe.get('product', 'Неизвестный товар')
                issue_text = pe.get('issue', 'Проблема с ценой')
                issues.append(f"Позиция '{product_name}': {issue_text}")

        return issues

    @staticmethod
    def _collect_error_stats(errors: List[Dict[str, Any]]) -> Dict[str, int]:
        labels = {
            'owner_error': 'Владелец',
            'source_error': 'Источник продажи',
            'channel_error': 'Канал продаж',
            'project_error': 'Проект',
            'contract_error': 'Договор',
            'contract_fields_error': 'Поля договора',
            'payment_method_error': 'Метод расчета',
            'payment_error': 'Оплата',
            'phone_error': 'Телефон',
            'pd_agreement_error': 'Соглашение ПД',
            'pd_date_error': 'Дата соглашения ПД',
            'unp_error': 'УНП/ИНН',
            'actual_address_error': 'Фактический адрес',
            'groups_error': 'Группа контрагентов',
            'type_name_mismatch_error': 'Тип ↔ Наименование'
        }

        stats: Dict[str, int] = defaultdict(int)
        for error in errors or []:
            for key, value in error.items():
                if key.endswith('_error') and value:
                    label = labels.get(key, key.replace('_', ' ').capitalize())
                    stats[label] += 1
            if error.get('price_errors'):
                stats['Цены'] += 1

        return dict(stats)

    @staticmethod
    def _generate_excel_report(document: str, region: str, date_from: date, date_to: date, errors: List[Dict[str, Any]]) -> Path:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        wb = Workbook()
        ws = wb.active
        ws.title = "Ошибки"

        # Для отгрузок добавляем разделение на основные проверки и проверки договоров
        if document == 'shipments':
            headers = [
                "#",
                "Документ",
                "Контрагент",
                "Дата",
                "Владелец",
                "Описание",
                "Основные проверки",
                "Проверки договоров",
                "Ошибка канала",
                "Ошибка проекта",
                "Ошибка источника",
                "Ошибка договора",
                "Ошибка полей договора",
                "Ошибка типа договора",
                "Ошибка метода расчета",
                "Ошибка оплаты",
                "Ссылка"
            ]
        else:
            headers = [
                "#",
                "Документ",
                "Контрагент",
                "Дата",
                "Владелец",
                "Описание",
                "Ошибка канала",
                "Ошибка проекта",
                "Ошибка источника",
                "Ошибка договора",
                "Ошибка полей договора",
                "Ошибка метода расчета",
                "Ошибка оплаты",
                "Ошибка телефона",
                "Ошибка согласия ПД",
                "Ошибка даты ПД",
                "Ошибка УНП/ИНН",
                "Ошибка фактического адреса",
                "Ошибка группы",
                "Ссылка"
            ]
        ws.append(headers)

        for idx, error in enumerate(errors, 1):
            name = error.get('name', 'Без названия')
            display_name = error.get('display_name')
            counterparty = error.get('counterparty', '')
            moment = error.get('moment', '')
            owner_display = error.get('owner', 'Не указан')

            issues = TelegramMonitoringBot._extract_issues(error)
            issues_text = " | ".join(issues) if issues else "Без описания"
            link = error.get('link', '')

            if document == 'shipments':
                # Для отгрузок разделяем на основные проверки и проверки договоров
                main_issues = error.get('main_issues', [])
                contract_issues = error.get('contract_issues', [])
                main_issues_text = " | ".join(main_issues) if main_issues else ""
                contract_issues_text = " | ".join(contract_issues) if contract_issues else ""
                
                ws.append([
                    idx,
                    display_name or name,
                    counterparty,
                    moment,
                    owner_display,
                    issues_text,
                    main_issues_text,
                    contract_issues_text,
                    error.get('channel_error', ''),
                    error.get('project_error', ''),
                    error.get('source_error', ''),
                    error.get('contract_error', ''),
                    error.get('contract_fields_error', ''),
                    error.get('contract_type_shipment_error', ''),
                    error.get('payment_method_error', ''),
                    error.get('payment_error', ''),
                    link
                ])
            else:
                ws.append([
                    idx,
                    display_name or name,
                    counterparty,
                    moment,
                    owner_display,
                    issues_text,
                    error.get('channel_error', ''),
                    error.get('project_error', ''),
                    error.get('source_error', ''),
                    error.get('contract_error', ''),
                    error.get('contract_fields_error', ''),
                    error.get('payment_method_error', ''),
                    error.get('payment_error', ''),
                    error.get('phone_error', ''),
                    error.get('pd_agreement_error', ''),
                    error.get('pd_date_error', ''),
                    error.get('unp_error', ''),
                    error.get('actual_address_error', ''),
                    error.get('groups_error', ''),
                    link
                ])

        safe_document = document.replace(' ', '_')
        filename = f"report_{safe_document}_{region}_{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}.xlsx"
        file_path = REPORTS_DIR / filename
        wb.save(file_path)
        return file_path

    @staticmethod
    def _build_message_chunks(
        document: str,
        region: str,
        date_from: date,
        date_to: date,
        result: Dict[str, Any],
        max_length: int | None = MAX_MESSAGE_LENGTH
    ) -> Tuple[List[str], Path | None]:
        """
        Формирует разобранный на части отчёт и при необходимости Excel с полным списком.
        """
        doc_name = DOCUMENTS.get(document, document)
        header = (
            f"{doc_name} {region.upper()} за {date_from.strftime('%d.%m.%Y')} - {date_to.strftime('%d.%m.%Y')}\n\n"
            f"Всего: {result.get('total', 0)}, "
            f"Валидных: {result.get('valid', 0)}, "
            f"Ошибок: {len(result.get('errors', []))}\n\n"
        )

        errors = result.get('errors', []) or []

        if not errors:
            return [header + '✅ Все документы в порядке!\n'], None

        stats = TelegramMonitoringBot._collect_error_stats(errors)

        blocks: List[str] = []
        if stats:
            stats_block = '📌 По типам ошибок:\n'
            for label, count in sorted(stats.items(), key=lambda item: (-item[1], item[0])):
                stats_block += f"• {label}: {count}\n"
            stats_block += '\n'
            blocks.append(stats_block)

        grouped = TelegramMonitoringBot._group_errors_by_owner(errors)
        has_truncated_owner = False

        for owner, owner_errors in grouped.items():
            owner_display = owner or 'Не указан'

            owner_block_lines = [
                f"- {owner_display}: {len(owner_errors)}\n"
            ]

            limit = MAX_DOCUMENTS_PER_OWNER if MAX_DOCUMENTS_PER_OWNER is not None else len(owner_errors)

            for error in owner_errors[:limit]:
                doc_display = error.get('display_name') or error.get('name') or 'Без названия'
                
                # Для отгрузок разделяем на основные проверки и проверки договоров
                if document == 'shipments':
                    main_issues = error.get('main_issues', [])
                    contract_issues = error.get('contract_issues', [])
                    
                    if main_issues or contract_issues:
                        owner_block_lines.append(f"  • {doc_display}:\n")
                        if main_issues:
                            main_text = '; '.join(main_issues)
                            owner_block_lines.append(f"    📋 Основные проверки: {main_text}\n")
                        if contract_issues:
                            contract_text = '; '.join(contract_issues)
                            owner_block_lines.append(f"    📄 Проверки договоров: {contract_text}\n")
                    else:
                        owner_block_lines.append(f"  • {doc_display}: Без описания\n")
                else:
                    issues = TelegramMonitoringBot._extract_issues(error)
                    issues_text = '; '.join(issues) if issues else 'Без описания'
                    owner_block_lines.append(f"  • {doc_display}: {issues_text}\n")
                
                link = error.get('link')
                if link:
                    owner_block_lines.append(f"    {link}\n")

            if MAX_DOCUMENTS_PER_OWNER is not None and len(owner_errors) > MAX_DOCUMENTS_PER_OWNER:
                remaining = len(owner_errors) - MAX_DOCUMENTS_PER_OWNER
                owner_block_lines.append(f"  ... и ещё {remaining} документов\n")
                has_truncated_owner = True

            owner_block_lines.append('\n')
            blocks.append(''.join(owner_block_lines))

        full_message = header + ''.join(blocks)

        chunks: List[str] = []
        excel_needed = False

        if max_length and max_length > 0:
            current = ''
            for line in full_message.splitlines(keepends=True):
                if len(line) > max_length:
                    # Сначала сбрасываем накопленное
                    if current.strip():
                        chunks.append(current.rstrip())
                        current = ''
                        excel_needed = True

                    segment_start = 0
                    while segment_start < len(line):
                        segment = line[segment_start:segment_start + max_length]
                        chunks.append(segment.rstrip())
                        segment_start += max_length
                    excel_needed = True
                    continue

                if len(current) + len(line) > max_length and current.strip():
                    chunks.append(current.rstrip())
                    current = ''
                    excel_needed = True

                current += line

            if current.strip():
                chunks.append(current.rstrip())
        else:
            chunks = [full_message.rstrip()]

        if len(chunks) > 1:
            excel_needed = True

        if has_truncated_owner:
            excel_needed = True

        # Добавляем пометку о вложении в первый подходящий блок
        if excel_needed and errors:
            attachment_note = "📎 Полный список ошибок во вложении."
            inserted_note = False
            for idx, chunk in enumerate(chunks):
                if max_length and max_length > 0:
                    if len(chunk) + len('\n\n' + attachment_note) <= max_length:
                        chunks[idx] = chunk + '\n\n' + attachment_note
                        inserted_note = True
                        break
                else:
                    chunks[idx] = chunk + '\n\n' + attachment_note
                    inserted_note = True
                    break
            if not inserted_note:
                chunks.append(attachment_note)

        if max_length and max_length > 0 and len(chunks) > 1:
            continuation_prefix = '⬇️ Продолжение отчёта\n\n'
            for idx in range(1, len(chunks)):
                chunk = chunks[idx]
                if len(chunk) + len(continuation_prefix) <= max_length:
                    chunks[idx] = continuation_prefix + chunk
                else:
                    chunks[idx] = chunk

        excel_path: Path | None = None
        if excel_needed and errors:
            excel_path = TelegramMonitoringBot._generate_excel_report(document, region, date_from, date_to, errors)

        return chunks, excel_path

    @staticmethod
    def _build_summary_message(
        document: str,
        region: str,
        date_from: date,
        date_to: date,
        result: Dict[str, Any],
        max_length: int = MAX_MESSAGE_LENGTH
    ) -> Tuple[str, Path | None]:
        chunks, excel_path = TelegramMonitoringBot._build_message_chunks(
            document,
            region,
            date_from,
            date_to,
            result,
            max_length
        )
        message = '\n\n'.join(chunk for chunk in chunks if chunk)
        return message, excel_path

    @staticmethod
    def _format_bitrix_message(
        document: str,
        region: str,
        date_from: date,
        date_to: date,
        result: Dict[str, Any]
    ) -> Tuple[str, Path | None]:
        message, excel_path = TelegramMonitoringBot._build_summary_message(
            document,
            region,
            date_from,
            date_to,
            result,
            MAX_MESSAGE_LENGTH
        )
        return message, excel_path

    def _send_results_to_bitrix(
        self,
        service: MonitoringServiceV2,
        document: str,
        region: str,
        date_from: date,
        date_to: date,
        result: Dict[str, Any]
    ) -> None:
        message, excel_path = self._format_bitrix_message(document, region, date_from, date_to, result)
        service.bitrix24_client.send_message_to_chat(message)

        if excel_path:
            caption = "📎 Полный список ошибок"
            service.bitrix24_client.send_file_to_chat(excel_path, caption)
    
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Начало работы с ботом"""
        user = update.effective_user

        if not self._is_user_allowed(user.id):
            logger.warning(f"Пользователь {user.id} ({user.username}) попытался получить доступ без разрешения")
            await update.message.reply_text(
                '⛔️ У вас нет доступа к этому боту. Если это ошибка, обратитесь к администратору.'
            )
            return ConversationHandler.END

        logger.info(f"Пользователь {user.id} ({user.username}) начал работу с ботом")
        
        # Создаем клавиатуру для выбора региона
        keyboard = [
            [InlineKeyboardButton(REGIONS['rb'], callback_data='region_rb')],
            [InlineKeyboardButton(REGIONS['rf'], callback_data='region_rf')],
            [InlineKeyboardButton(REGIONS['kz'], callback_data='region_kz')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            '🔍 *Мониторинг документов МойСклад*\n\n'
            'Добро пожаловать! Этот бот поможет вам проверить документы в МойСклад.\n\n'
            '*Шаг 1/4:* Выберите регион:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return REGION
    
    async def region_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка выбора региона"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        region = query.data.replace('region_', '')
        context.user_data['region'] = region
        
        logger.info(f"Пользователь {query.from_user.id} выбрал регион: {region.upper()}")
        
        # Создаем клавиатуру для выбора документа
        keyboard = [
            [InlineKeyboardButton(DOCUMENTS['shipments'], callback_data='doc_shipments')],
            [InlineKeyboardButton(DOCUMENTS['sales'], callback_data='doc_sales')],
            [InlineKeyboardButton(DOCUMENTS['commission'], callback_data='doc_commission')],
            [InlineKeyboardButton(DOCUMENTS['contractors'], callback_data='doc_contractors')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='back_to_region')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f'✅ Регион: *{REGIONS[region]}*\n\n'
            f'*Шаг 2/4:* Выберите тип документа:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return DOCUMENT
    
    async def document_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка выбора документа"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        if query.data == 'back_to_region':
            return await self.back_to_region(update, context)
        
        document = query.data.replace('doc_', '')
        context.user_data['document'] = document
        
        logger.info(f"Пользователь {query.from_user.id} выбрал документ: {document}")
        
        # Предлагаем быстрые варианты периода
        reply_markup = self._create_period_keyboard()
        
        region_name = REGIONS[context.user_data['region']]
        doc_name = DOCUMENTS[document]
        
        await query.edit_message_text(
            f'✅ Регион: *{region_name}*\n'
            f'✅ Документ: *{doc_name}*\n\n'
            f'*Шаг 3/4:* Выберите период проверки:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return DATE_FROM
    
    async def period_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка выбора периода"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        if query.data == 'back_to_document':
            return await self.back_to_document(update, context)
        
        if query.data == 'period_custom':
            await query.edit_message_text(
                '📅 Введите дату начала периода в формате *ДД.ММ.ГГГГ*\n'
                'Например: 01.10.2025\n\n'
                'Или отправьте /cancel для отмены',
                parse_mode='Markdown'
            )
            return DATE_FROM
        
        # Быстрые варианты периода
        today = date.today()
        
        if query.data == 'period_today':
            date_from = today
            date_to = today
        elif query.data == 'period_yesterday':
            date_from = today - timedelta(days=1)
            date_to = today - timedelta(days=1)
        elif query.data == 'period_3days':
            date_from = today - timedelta(days=2)
            date_to = today
        elif query.data == 'period_week':
            date_from = today - timedelta(days=6)
            date_to = today
        elif query.data == 'period_month':
            date_from = today - timedelta(days=29)
            date_to = today
        else:
            await query.edit_message_text('❌ Неизвестный период')
            return ConversationHandler.END
        
        context.user_data['date_from'] = date_from
        context.user_data['date_to'] = date_to
        
        # Переходим к выбору отправки в Bitrix24
        return await self.ask_bitrix_option(update, context, via_callback=True)
    
    async def date_from_received(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка ввода даты начала"""
        user = update.effective_user
        if not self._is_user_allowed(user.id):
            await update.message.reply_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END

        try:
            date_from = datetime.strptime(update.message.text, '%d.%m.%Y').date()
            context.user_data['date_from'] = date_from
            
            await update.message.reply_text(
                f'✅ Дата начала: *{date_from.strftime("%d.%m.%Y")}*\n\n'
                f'📅 Теперь введите дату окончания периода в формате *ДД.ММ.ГГГГ*\n'
                f'Например: {date.today().strftime("%d.%m.%Y")}\n\n'
                f'Или отправьте /cancel для отмены',
                parse_mode='Markdown'
            )
            return DATE_TO
        except ValueError:
            await update.message.reply_text(
                '❌ Неверный формат даты. Попробуйте еще раз.\n'
                'Формат: *ДД.ММ.ГГГГ* (например, 01.10.2025)\n\n'
                'Или отправьте /cancel для отмены',
                parse_mode='Markdown'
            )
            return DATE_FROM
    
    async def date_to_received(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка ввода даты окончания"""
        user = update.effective_user
        if not self._is_user_allowed(user.id):
            await update.message.reply_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END

        try:
            date_to = datetime.strptime(update.message.text, '%d.%m.%Y').date()
            date_from = context.user_data['date_from']
            
            if date_to < date_from:
                await update.message.reply_text(
                    '❌ Дата окончания не может быть раньше даты начала.\n'
                    'Попробуйте еще раз или отправьте /cancel для отмены',
                    parse_mode='Markdown'
                )
                return DATE_TO
            
            context.user_data['date_to'] = date_to
            
            # Переходим к выбору отправки в Bitrix24
            return await self.ask_bitrix_option(update, context, via_callback=False)
            
        except ValueError:
            await update.message.reply_text(
                '❌ Неверный формат даты. Попробуйте еще раз.\n'
                'Формат: *ДД.ММ.ГГГГ* (например, 01.10.2025)\n\n'
                'Или отправьте /cancel для отмены',
                parse_mode='Markdown'
            )
            return DATE_TO
    
    async def ask_bitrix_option(self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, via_callback: bool) -> int:
        """Предлагаем выбрать отправку результатов в Bitrix24"""
        region = context.user_data['region']
        document = context.user_data['document']
        date_from = context.user_data['date_from']
        date_to = context.user_data['date_to']

        region_name = REGIONS[region]
        doc_name = DOCUMENTS[document]

        keyboard = [
            [InlineKeyboardButton('📤 Да, отправить в Bitrix24', callback_data='bitrix_yes')],
            [InlineKeyboardButton('💬 Нет, только в Telegram', callback_data='bitrix_no')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='back_to_period')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        text = (
            f'✅ Регион: *{region_name}*\n'
            f'✅ Документ: *{doc_name}*\n'
            f'✅ Период: *{date_from.strftime("%d.%m.%Y")} - {date_to.strftime("%d.%m.%Y")}*\n\n'
            f'*Шаг 4/4:* Отправлять результат в Bitrix24?'
        )

        if via_callback and update.callback_query:
            await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        elif update.message:
            await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')
        else:
            logger.warning('Не удалось определить тип обновления для выбора Bitrix24')
            return ConversationHandler.END

        return BITRIX

    async def run_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Запуск проверки документов"""
        user = update.effective_user
        if not self._is_user_allowed(user.id):
            if update.callback_query:
                await update.callback_query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            elif update.message:
                await update.message.reply_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        # Получаем данные
        region = context.user_data['region']
        document = context.user_data['document']
        date_from = context.user_data['date_from']
        date_to = context.user_data['date_to']
        
        region_name = REGIONS[region]
        doc_name = DOCUMENTS[document]
        
        # Отправляем сообщение о начале проверки
        if update.callback_query:
            message = await update.callback_query.edit_message_text(
                f'🔄 *Запуск проверки...*\n\n'
                f'Регион: {region_name}\n'
                f'Документ: {doc_name}\n'
                f'Период: {date_from.strftime("%d.%m.%Y")} - {date_to.strftime("%d.%m.%Y")}\n\n'
                f'⏳ Пожалуйста, подождите...',
                parse_mode='Markdown'
            )
        else:
            message = await update.message.reply_text(
                f'🔄 *Запуск проверки...*\n\n'
                f'Регион: {region_name}\n'
                f'Документ: {doc_name}\n'
                f'Период: {date_from.strftime("%d.%m.%Y")} - {date_to.strftime("%d.%m.%Y")}\n\n'
                f'⏳ Пожалуйста, подождите...',
                parse_mode='Markdown'
            )
        
        try:
            # Получаем сервис мониторинга
            service = self.get_service(region)
            
            # Запускаем проверку в зависимости от типа документа
            if document == 'shipments':
                result = service.check_shipments_period(date_from, date_to)
            elif document == 'sales':
                result = service.check_sales_period(date_from, date_to)
            elif document == 'commission':
                result = service.check_commission_reports_period(date_from, date_to)
            elif document == 'contractors':
                result = service.check_contractors_period(date_from, date_to)
            else:
                await message.edit_text('❌ Неизвестный тип документа')
                return ConversationHandler.END
            
            # Формируем отчет
            if result.get('status') == 'success':
                total = result.get('total', 0)
                valid = result.get('valid', 0)
                errors = result.get('errors', [])
                send_to_bitrix_flag = context.user_data.get('send_to_bitrix', False)
                bitrix_sent = False
                bitrix_error_text = None
                
                header = '✅ *Проверка завершена*\n\n'
                remaining_length = max(MAX_MESSAGE_LENGTH - len(header), 0)
                chunks, excel_path = self._build_message_chunks(
                    document,
                    region,
                    date_from,
                    date_to,
                    result,
                    remaining_length
                )
                if not chunks:
                    chunks = ['']

                report = header + (chunks[0] if chunks else '')
 
                if send_to_bitrix_flag:
                    try:
                        self._send_results_to_bitrix(service, document, region, date_from, date_to, result)
                        bitrix_sent = True
                        report += '\n\n📤 Результаты отправлены в Bitrix24.'
                        logger.info(
                            f"Результаты автоматически отправлены в Bitrix24 по запросу пользователя {user.id}"
                        )
                    except Exception as bitrix_exc:
                        bitrix_error_text = str(bitrix_exc)
                        report += (
                            '\n\n❗️ Не удалось автоматически отправить в Bitrix24. '
                            'Вы можете попробовать еще раз вручную.'
                        )
                        logger.error(f"Ошибка автоматической отправки в Bitrix24: {bitrix_exc}", exc_info=True)
                
                # Добавляем кнопки для новой проверки
                keyboard = [
                    [InlineKeyboardButton('🔄 Новая проверка', callback_data='new_check')]
                ]

                if bitrix_sent:
                    keyboard.append([
                        InlineKeyboardButton('📊 Отправить повторно в Bitrix24', callback_data='send_to_bitrix')
                    ])
                else:
                    keyboard.append([
                        InlineKeyboardButton('📊 Отправить в Bitrix24', callback_data='send_to_bitrix')
                    ])
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await message.edit_text(report, reply_markup=reply_markup, parse_mode='Markdown')
                
                # Отправляем оставшиеся части отчёта отдельными сообщениями
                for chunk in chunks[1:]:
                    if chunk.strip():
                        await context.bot.send_message(
                            chat_id=update.effective_chat.id,
                            text=chunk,
                            parse_mode='Markdown'
                        )

                # Сохраняем результат для возможной отправки в Bitrix24
                context.user_data['last_result'] = result
                if bitrix_error_text:
                    context.user_data['last_bitrix_error'] = bitrix_error_text
                else:
                    context.user_data.pop('last_bitrix_error', None)
                context.user_data.pop('send_to_bitrix', None)

                # Отправляем Excel-файл, если он был сформирован для полного списка
                if excel_path:
                    try:
                        with open(excel_path, "rb") as fh:
                            await context.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=fh,
                                filename=os.path.basename(excel_path),
                                caption="📎 Полный список ошибок"
                            )
                    finally:
                        try:
                            Path(excel_path).unlink()
                        except Exception:
                            pass
 
            else:
                # Обрабатываем ошибки (status == 'error')
                error_msg = result.get('error') or result.get('error_message') or 'Неизвестная ошибка'
                await message.edit_text(
                    f'❌ *Ошибка при проверке*\n\n'
                    f'{error_msg}\n\n'
                    f'Попробуйте позже или обратитесь к администратору.',
                    parse_mode='Markdown'
                )
            
            logger.info(f"Проверка завершена для пользователя {update.effective_user.id}: {document} {region.upper()}")
            
        except Exception as e:
            logger.error(f"Ошибка при выполнении проверки: {e}", exc_info=True)
            await message.edit_text(
                f'❌ *Произошла ошибка*\n\n'
                f'Не удалось выполнить проверку. Попробуйте позже.\n\n'
                f'Ошибка: {str(e)}',
                parse_mode='Markdown'
            )
        
        return ConversationHandler.END

    async def bitrix_selected(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Обработка выбора отправки в Bitrix24"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END

        choice = query.data

        if choice == 'back_to_period':
            # Возвращаемся к выбору периода
            context.user_data.pop('date_from', None)
            context.user_data.pop('date_to', None)
            context.user_data.pop('send_to_bitrix', None)

            region = context.user_data.get('region', 'rb')
            document = context.user_data.get('document', 'shipments')

            region_name = REGIONS[region]
            doc_name = DOCUMENTS[document]

            reply_markup = self._create_period_keyboard()

            await query.edit_message_text(
                f'✅ Регион: *{region_name}*\n'
                f'✅ Документ: *{doc_name}*\n\n'
                f'*Шаг 3/4:* Выберите период проверки:',
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )

            return DATE_FROM

        context.user_data['send_to_bitrix'] = choice == 'bitrix_yes'

        return await self.run_check(update, context)
    
    async def send_to_bitrix(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Отправка результатов в Bitrix24"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        result = context.user_data.get('last_result')
        if not result:
            await query.edit_message_text('❌ Нет сохраненных результатов для отправки')
            return ConversationHandler.END
        
        try:
            region = context.user_data['region']
            document = context.user_data['document']
            date_from = context.user_data['date_from']
            date_to = context.user_data['date_to']
            
            service = self.get_service(region)
            self._send_results_to_bitrix(service, document, region, date_from, date_to, result)
            
            await query.edit_message_text(
                f'✅ Результаты успешно отправлены в Bitrix24!\n\n'
                f'Для новой проверки используйте команду /start',
                parse_mode='Markdown'
            )
            
            context.user_data.pop('last_bitrix_error', None)

            logger.info(f"Результаты отправлены в Bitrix24 пользователем {query.from_user.id}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки в Bitrix24: {e}", exc_info=True)
            await query.edit_message_text(
                f'❌ Ошибка отправки в Bitrix24\n\n'
                f'{str(e)}',
                parse_mode='Markdown'
            )
        
        return ConversationHandler.END
    
    async def new_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Начать новую проверку"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        # Очищаем данные пользователя
        context.user_data.clear()
        
        # Создаем клавиатуру для выбора региона
        keyboard = [
            [InlineKeyboardButton(REGIONS['rb'], callback_data='region_rb')],
            [InlineKeyboardButton(REGIONS['rf'], callback_data='region_rf')],
            [InlineKeyboardButton(REGIONS['kz'], callback_data='region_kz')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            '🔍 *Новая проверка*\n\n'
            '*Шаг 1/4:* Выберите регион:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return REGION
    
    async def back_to_region(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Вернуться к выбору региона"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        keyboard = [
            [InlineKeyboardButton(REGIONS['rb'], callback_data='region_rb')],
            [InlineKeyboardButton(REGIONS['rf'], callback_data='region_rf')],
            [InlineKeyboardButton(REGIONS['kz'], callback_data='region_kz')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            '🔍 *Мониторинг документов МойСклад*\n\n'
            '*Шаг 1/4:* Выберите регион:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return REGION
    
    async def back_to_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Вернуться к выбору документа"""
        query = update.callback_query
        await query.answer()

        if not self._is_user_allowed(query.from_user.id):
            await query.edit_message_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return ConversationHandler.END
        
        keyboard = [
            [InlineKeyboardButton(DOCUMENTS['shipments'], callback_data='doc_shipments')],
            [InlineKeyboardButton(DOCUMENTS['sales'], callback_data='doc_sales')],
            [InlineKeyboardButton(DOCUMENTS['commission'], callback_data='doc_commission')],
            [InlineKeyboardButton(DOCUMENTS['contractors'], callback_data='doc_contractors')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='back_to_region')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        region = context.user_data.get('region', 'rb')
        await query.edit_message_text(
            f'✅ Регион: *{REGIONS[region]}*\n\n'
            f'*Шаг 2/4:* Выберите тип документа:',
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        
        return DOCUMENT
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Отмена операции"""
        await update.message.reply_text(
            '❌ Операция отменена.\n\n'
            'Для новой проверки используйте команду /start',
            parse_mode='Markdown'
        )
        context.user_data.clear()
        return ConversationHandler.END
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Справка по командам"""
        user = update.effective_user
        if not self._is_user_allowed(user.id):
            await update.message.reply_text('⛔️ Нет доступа к боту. Обратитесь к администратору.')
            return

        help_text = (
            '📖 *Справка по боту мониторинга МойСклад*\n\n'
            '*Доступные команды:*\n'
            '/start - Начать проверку документов\n'
            '/help - Показать эту справку\n'
            '/cancel - Отменить текущую операцию\n\n'
            '*Возможности бота:*\n'
            '• Проверка документов в МойСклад\n'
            '• Поддержка 3 регионов: РБ, РФ, КЗ\n'
            '• 4 типа документов: отгрузки, продажи, отчеты комиссионеров, контрагенты\n'
            '• Гибкая настройка периода проверки\n'
            '• Отправка результатов в Bitrix24\n\n'
            '*Как использовать:*\n'
            '1. Отправьте /start\n'
            '2. Выберите регион\n'
            '3. Выберите тип документа\n'
            '4. Укажите период проверки\n'
            '5. Получите результаты и при необходимости отправьте в Bitrix24'
        )
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    def run(self):
        """Запуск бота"""
        # Создаем приложение
        application = Application.builder().token(self.token).build()
        
        # Создаем обработчик разговора
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler('start', self.start),
                CallbackQueryHandler(self.region_selected, pattern='^region_')
            ],
            states={
                REGION: [
                    CallbackQueryHandler(self.region_selected, pattern='^region_')
                ],
                DOCUMENT: [
                    CallbackQueryHandler(self.document_selected, pattern='^(doc_|back_to_region)')
                ],
                DATE_FROM: [
                    CallbackQueryHandler(self.period_selected, pattern='^(period_|back_to_document)'),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.date_from_received)
                ],
                DATE_TO: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.date_to_received)
                ],
                BITRIX: [
                    CallbackQueryHandler(self.bitrix_selected, pattern='^(bitrix_|back_to_period)')
                ]
            },
            fallbacks=[CommandHandler('cancel', self.cancel)]
        )
        
        # Добавляем обработчики
        application.add_handler(conv_handler)
        application.add_handler(CommandHandler('help', self.help_command))
        application.add_handler(CallbackQueryHandler(self.new_check, pattern='^new_check$'))
        application.add_handler(CallbackQueryHandler(self.send_to_bitrix, pattern='^send_to_bitrix$'))
        
        logger.info("Telegram бот запущен")
        print("🤖 Telegram бот мониторинга МойСклад запущен!")
        
        # Запускаем бота
        application.run_polling(allowed_updates=Update.ALL_TYPES)


def main():
    """Точка входа"""
    bot = TelegramMonitoringBot()
    bot.run()


if __name__ == '__main__':
    main()

