# Выполненная проверка — 29 октября 2026

## Go

- Go toolchain: go version go1.27.3 linux/amd64.
- go vet ./...: пройдено.
- go test -race -cover ./...: 20 основных тестов и 4 подслучая, без ошибок и обнаруженных гонок.
- Проверены temporal split, исключение виденных товаров, cold start, детерминированные веса, quality gate, ухудшение относительно champion, идемпотентность, конфликт ID, конкурентная запись, сохранение после перезапуска, ручной и автоматический откат, HTTP API и ошибки MLflow.
- MLflow REST client проверен локальным тестовым HTTP-сервером, включая upload обоих JSON-артефактов. Это не запуск настоящего MLflow.

Покрытие проверенных пакетов:

```text
ok  	recogo/internal/tracking	1.024s	coverage: 83.1% of statements
ok  	recogo/internal/reco	1.036s	coverage: 95.2% of statements
ok  	recogo/internal/app	1.264s	coverage: 72.3% of statements
```

У cmd/recogo и internal/stream нет unit test coverage; реальный CLI дополнительно проверен ниже. Kafka consumer с настоящим брокером здесь не запускался.

## Локальный исполняемый файл

Linux-бинарник запущен отдельным процессом на 127.0.0.1. CLI seed сохранил 290 событий, CLI train создал две разные принятые версии. Проверены выдача user-001, fallback нового пользователя, список версий и откат на первую модель. Сервер штатно завершился с кодом 0.

Оценка синтетического примера: 58 пользователей, item-to-item HitRate@5 = 1, popularity baseline = 0. Данные намеренно простые; эти результаты не описывают качество реального магазина. Реальные ответы сохранены в example_run.json.

Windows x64 exe успешно кросс-скомпилирован, Windows runtime и bat-файлы в этом Linux-окружении не выполнялись. Linux x64 бинарник собран с CGO_ENABLED=0. Контрольные суммы — bin/SHA256SUMS.txt.

## Конфигурации

Compose, workflow CI и Kubernetes-манифесты разобраны YAML-парсером. Это проверка синтаксиса YAML, а не docker compose config или валидация схемы Kubernetes.

Полный Docker Compose, настоящий Kafka, MLflow, Airflow и Kubernetes здесь не запускались: Docker/Kubernetes в окружении отсутствуют. Для полного интеграционного сценария добавлен GitHub Actions workflow; его запуск ещё не подтверждён. Перед использованием полного стенда выполните инструкции на машине с Docker и проверьте логи.
