#!/usr/bin/env bash

# Скрипт для запуска демо-клиента прямо внутри контейнера Докера
# Так пользователю не нужно иметь Go на своем компьютере

if ! docker compose ps | grep -q "queue-manager"; then
  echo "Ошибка: Контейнеры docker-compose не запущены!"
  echo "Сначала запусти кластер: docker compose up -d"
  exit 1
fi

# Проксируем все переданные аргументы внутрь контейнера
docker compose exec queue-manager /app/demo "$@"
