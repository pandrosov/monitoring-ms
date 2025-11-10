IMAGE_NAME ?= monitoring-ms
ENV_FILE ?= .env
LOGS_DIR ?= $(PWD)/logs
DOCKER_CMD ?= python main_v2.py
DETACH ?= 0

RUN_FLAGS := --rm
ifeq ($(DETACH),1)
RUN_FLAGS += -d
endif

.PHONY: help build build-no-cache run run-detached monitor monitor-detached bot bot-detached shell logs

help:
	@echo ""
	@echo "Доступные цели:"
	@echo "  help                - показать это сообщение"
	@echo "  build               - собрать Docker-образ ($(IMAGE_NAME))"
	@echo "  build-no-cache      - собрать образ без использования кэша"
	@echo "  run                 - запустить основной сценарий (DOCKER_CMD=$(DOCKER_CMD))"
	@echo "  monitor             - запустить мониторинг (параметры через ARGS=\"--region RB ...\")"
	@echo "  bot                 - запустить Telegram-бота"
	@echo "  shell               - открыть bash внутри контейнера"
	@echo ""
	@echo "Запуск в фоне: добавьте DETACH=1 (например, 'make run DETACH=1') или используйте цели *-detached."
	@echo "Файлы логов примонтированы из $(LOGS_DIR)."
	@echo ""

build:
	docker build -t $(IMAGE_NAME) .

build-no-cache:
	docker build --no-cache -t $(IMAGE_NAME) .

run: logs
	docker run $(RUN_FLAGS) --env-file $(ENV_FILE) -v $(LOGS_DIR):/app/logs $(IMAGE_NAME) $(DOCKER_CMD)

run-detached: DETACH = 1
run-detached: run

monitor: logs
	docker run $(RUN_FLAGS) --env-file $(ENV_FILE) -v $(LOGS_DIR):/app/logs $(IMAGE_NAME) python run_monitoring.py $(ARGS)

monitor-detached: DETACH = 1
monitor-detached: monitor

bot: logs
	docker run $(RUN_FLAGS) --env-file $(ENV_FILE) -v $(LOGS_DIR):/app/logs $(IMAGE_NAME) python telegram_bot.py

bot-detached: DETACH = 1
bot-detached: bot

shell: logs
	docker run --rm -it --env-file $(ENV_FILE) -v $(LOGS_DIR):/app/logs $(IMAGE_NAME) /bin/bash

logs:
	mkdir -p $(LOGS_DIR)

