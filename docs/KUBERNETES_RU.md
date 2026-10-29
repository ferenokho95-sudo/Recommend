# Kubernetes

Манифесты рассчитаны на учебный кластер с StorageClass по умолчанию, например minikube или kind с provisioner. Это конфигурация для запуска, не проверенная здесь установка кластера.

## Образ

```bash
docker build -t recogo:local .
kind load docker-image recogo:local
```

Для minikube вместо kind используйте `minikube image load recogo:local`. В удалённом кластере отправьте образ в свой registry и замените image в Deployment и CronJob.

## Установка

```bash
kubectl apply -f deploy/k8s/app.yaml
kubectl apply -f deploy/k8s/kafka.yaml
kubectl apply -f deploy/k8s/mlflow.yaml
kubectl -n recogo wait --for=condition=complete job/kafka-topics --timeout=300s
kubectl -n recogo rollout status deployment/mlflow --timeout=300s
kubectl -n recogo rollout status deployment/recogo --timeout=300s
```

Сервис может перезапуститься, пока Kafka ещё не готова: Kubernetes восстановит контейнер. Проверьте `kubectl -n recogo get pods,jobs,pvc` и логи.

Deployment работает в одном экземпляре, стратегией Recreate, с PVC. bbolt нельзя одновременно открывать несколькими процессами и размещать на ненадёжной сетевой файловой системе. Readiness probe использует `/healthz`, чтобы API мог принимать события до первой модели. Готовность модели отдельно проверяется `/readyz`.

## Данные и обучение

```bash
kubectl -n recogo exec deployment/recogo -- /recogo seed --url http://127.0.0.1:8080
kubectl -n recogo exec deployment/recogo -- /recogo train --url http://127.0.0.1:8080
kubectl -n recogo port-forward service/recogo 8080:8080
```

Панель: http://127.0.0.1:8080. MLflow можно открыть отдельным port-forward: `kubectl -n recogo port-forward service/mlflow 5000:5000`.

CronJob `recogo-retrain` запускает Go CLI каждый час в часовом поясе контроллера кластера (обычно UTC). Он обращается к API, не открывая bbolt-файл. Встроенный ticker отключён через TRAIN_INTERVAL=0s. Токен берётся из Secret. При отклонении модели CLI возвращает ненулевой код, Job считается неуспешным.

## Откат

```bash
kubectl -n recogo exec deployment/recogo -- /recogo rollback --url http://127.0.0.1:8080
```

Это откат версии модели. Откат контейнера приложения выполняется отдельно: `kubectl -n recogo rollout undo deployment/recogo`. Для образов приложения используйте уникальные теги; recogo:local предназначен только для локальной демонстрации.

Перед внешним доступом замените ADMIN_TOKEN в Secret, добавьте TLS, аутентификацию и права. Kafka, MLflow и SQLite backend MLflow здесь одноузловые и не обеспечивают HA. Начальные requests/limits — пример конфигурации, а не результат capacity planning.
