# 🧬 Genomic VCF Ingestion & AI Insight Engine — Полный гайд по запуску и верификации

Этот проект представляет собой **Production-Ready MVP** платформы для ингестии геномных VCF-файлов, их высокопроизводительного парсинга на Rust и выдачи точных научно обоснованных ответов пользователю с помощью AI-агента с **Tool Calling** (без галлюцинаций) на базе сверхбыстрых моделей **Cerebras** и локальной базы **DuckDB**.

---

## 🏗 Архитектура и стек технологий

1. **Rust Ingestion Worker (`/rust-ingestion-worker`)**:
   - Мультикор-парсинг VCF с помощью `Rayon`.
   - Асинхронная интеграция с Temporal через `Tokio` (отправка `heartbeat(...)` каждые 500 мс).
   - Экспорт генотипов в аналитическую колонно-ориентированную базу **DuckDB**.
2. **TS API Agent & HTTP Server (`/ts-api-agent`)**:
   - Работает на **чистом Node.js (24/25)** с нативной поддержкой TypeScript без транспиляторов.
   - Встроенный HTTP-адаптер (`node:http` + `Hono`) без внешних зависимостей сервера.
   - Интеграция с **Cerebras API** (`llama-3.3-70b` / `llama-3.1-70b`) и автоматическое выполнение SQL JOIN-запросов к DuckDB через инструмент `query_genotype`.
3. **Temporal Orchestration**:
   - Полный мониторинг каждого шага загрузки и обработки в **Temporal UI**.

---

## 🚀 1. Быстрая автономная проверка (без Docker и внешних сервисов)

Для проверки всей математики, парсера DuckDB и детерминированного AI-ответа в терминале:

```bash
# 1. Инициализация DuckDB из фикстур (demo_user.vcf + annotations_mock.tsv)
make init-db

# 2. Запуск интеграционного E2E-теста
make test-e2e
```
*Что произойдёт:* `make test-e2e` сымитирует вопрос `"Can I drink coffee?"`, вызовет инструмент `query_genotype("CYP1A2")`, извлечёт генотип **`C/C`** (`Slow caffeine metabolizer`, сниженная активность фермента) и вернёт проверенный детерминированный ответ.

---

## 📊 2. Мониторинг пайплайна ингестии в Temporal UI (`http://localhost:8233`)

Для визуального наблюдения за 3-этапным Workflow (`downloadVcf` ➔ `parseAndIndexVcf` ➔ `validateDataset`):

### Вкладка 1: Локальный Temporal Dev Server (через Homebrew)
```bash
make temporal-dev
```
*Сервер запустится на порту 7233, а веб-интерфейс **Temporal UI откроется по адресу http://localhost:8233***.

### Вкладка 2: Запуск Temporal Worker (обработчик очереди `genomic-ingestion`)
```bash
make worker
```
*Воркер подключится к серверу и начнёт слушать очередь `genomic-ingestion`. Он автоматически находит скомпилированный бинарник Rust (`target/release/rust-ingestion-worker`) и не требует вызова `cargo run`.*

### Вкладка 3: Отправка задачи на ингестию
```bash
make run-workflow
```
*В консоль будет выведена прямая ссылка на исполнение. Перейдя в **http://localhost:8233**, ты увидишь Workflow со статусом **COMPLETED** и зелёными галочками напротив каждого шага.*

---

## 🧠 3. Запуск HTTP API-сервера с ключом Cerebras (`llama-3.3-70b`)

Чтобы протестировать реальную генерацию со сверхбыстрой моделью Cerebras:

### Шаг 1. Экспорт ключа в терминале
```bash
export CEREBRAS_API_KEY="твой-ключ-cerebras"
# Опционально: выбор модели (по умолчанию используется llama-3.3-70b)
export CEREBRAS_MODEL="llama-3.3-70b"
```

### Шаг 2. Запуск HTTP-сервера агента
```bash
make run-ts-api
```
*Сервер поднимется на **http://localhost:3000**.*

### Шаг 3. Реальный запрос через `curl` (из другой вкладки терминала)
```bash
curl -X POST http://localhost:3000/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Can I drink coffee? Check my CYP1A2 genetic variant and explain what it means for my diet."
  }'
```

#### Как работает обработка вопроса:
1. **Tool Calling**: Cerebras анализирует запрос пользователя и возвращает вызов функции:
   ```json
   { "name": "query_genotype", "arguments": "{\"targetId\":\"CYP1A2\"}" }
   ```
2. **DuckDB JOIN**: Сервер выполняет детерминированный SQL-запрос, объединяя данные пользователя (`user_variants`) с клинической аннотацией (`clinvar_annotations`).
3. **Отсутствие галлюцинаций**: Найденные факты (`Genotype: C/C`, `Clinical Significance: Risk Factor`, `Phenotype: Slow caffeine metabolizer`) отправляются обратно в Cerebras.
4. **Финальный ответ**: Модель формирует точную медицинско-биологическую рекомендацию на скорости 2000+ токенов/секунду.

---

## 📋 4. Шпаргалка команд `Makefile`

| Команда | Описание |
| :--- | :--- |
| `make init-db` | Инициализация DuckDB из фикстур (`demo_user.vcf` + `annotations_mock.tsv`) |
| `make test-e2e` | Запуск интеграционного теста от запроса до проверки ответа |
| `make temporal-dev` | Запуск локального Temporal Dev Server (UI на порту 8233) |
| `make worker` | Запуск TypeScript/Rust воркера для очереди `genomic-ingestion` |
| `make run-workflow` | Запуск тестового Workflow ингестии геномных данных |
| `make run-ts-api` | Запуск HTTP REST API-сервера на порту 3000 |
| `make build` | Сборка Rust (`cargo build --release`) и TS-проектов |
| `make run-local` | Полная локальная верификация (инициализация БД + E2E тест) |
| `make clean` | Очистка сборок и временных файлов баз данных (`*.duckdb`, `temporal.sqlite`) |

---

## 🛠 Решение частых вопросов

- **Почему Workflow в Temporal UI показывает статус `Running`?**
  Убедись, что в отдельном терминале запущен обработчик (`make worker`). Как только воркер поднимется, он мгновенно подхватит задачу и завершит её.
- **Где лежит скомпилированный Rust-бинарник?**
  В `target/release/rust-ingestion-worker`. Активность `parseAndIndexVcf` проверяет этот путь автоматически, благодаря чему задачи в Temporal выполняются за миллисекунды.
- **Как проверить статус API без отправки вопроса?**
  Выполни `curl http://localhost:3000/health` — сервер вернёт `{"status":"ok","service":"ts-api-agent"}`.
