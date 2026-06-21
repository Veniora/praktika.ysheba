#!/usr/bin/env bash

# Цвета для вывода в консоль
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${CYAN}=====================================================${NC}"
echo -e "${CYAN}      Панель быстрого запуска Брокера Сообщений      ${NC}"
echo -e "${CYAN}=====================================================${NC}"
echo -e "Выбери вариант запуска:"
echo -e "1) Локальный запуск и авто-тестирование (требуется Go на хосте)"
echo -e "2) Запуск кластера в Docker Compose (Go не требуется)"
echo -e "3) Запустить пример отправки и чтения сообщений в запущенном Docker-кластере"
echo -e "4) Остановить контейнеры Docker и стереть данные"
echo -e "5) Выход"
echo -n "Введи номер варианта: "
read -r OPTION

case $OPTION in
  1)
    echo -e "\n${YELLOW}[*] Запуск локального авто-теста...${NC}"
    chmod +x test.sh
    ./test.sh
    ;;
  2)
    echo -e "\n${YELLOW}[*] Сборка и запуск кластера в Docker Compose...${NC}"
    docker compose up --build -d
    echo -e "${GREEN}[SUCCESS] Кластер запущен!${NC}"
    echo -e "Менеджер Очередей доступен на: http://localhost:8080"
    echo -e "Брокер 1 доступен на: http://localhost:8081"
    echo -e "Брокер 2 доступен на: http://localhost:8082"
    echo -e "\nПроверим статус кластера через 3 секунды..."
    sleep 3
    docker compose exec queue-manager /app/demo -mode status
    ;;
  3)
    echo -e "\n${YELLOW}[*] Запуск примера работы внутри Docker...${NC}"
    if ! docker compose ps | grep -q "queue-manager"; then
      echo -e "${RED}Ошибка: Докер-кластер не запущен! Сначала выбери пункт 2.${NC}"
      exit 1
    fi
    echo -e "\n1. Отправляем 5 сообщений в топик 'docker-topic'..."
    ./docker-demo.sh -mode publish -topic docker-topic -count 5 -payload "hello-from-docker"
    
    echo -e "\n2. Читаем эти 5 сообщений подписчиком из группы 'docker-group'..."
    ./docker-demo.sh -mode subscribe -topic docker-topic -group docker-group -id client-docker -count 5
    
    echo -e "\n3. Проверяем обновленный статус кластера:"
    ./docker-demo.sh -mode status
    ;;
  4)
    echo -e "\n${YELLOW}[*] Остановка Docker и удаление данных...${NC}"
    docker compose down -v
    rm -rf data logs bin
    echo -e "${GREEN}[SUCCESS] Все остановлено и очищено!${NC}"
    ;;
  5)
    echo "Выход."
    exit 0
    ;;
  *)
    echo -e "${RED}Неверный выбор.${NC}"
    exit 1
    ;;
esac
