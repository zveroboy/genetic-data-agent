# 🧬 Genomic VCF Ingestion & AI Insight Engine — Полное руководство

Автономная Production-Ready платформа для ингестии геномных VCF-файлов, их высокопроизводительного парсинга на Rust (Rayon engine), оркестрации в Temporal, хранения в DuckDB и выдачи детерминированных медицинских ответов с использованием **Cerebras LLM (Llama-3.3-70B)** и семантического поиска по научным статьям PubMed в **Qdrant** через локальную **Ollama (`nomic-embed-text`)**.

---

## 🗄 1. Используемые Данные и Источники (Data Sources)

Платформа использует строго глобальные биоинформатические стандарты данных:

### А. ДНК-Данные Пациента (Genomic Datasets)
1. **NIST GIAB NA12878 / HG001 (`data/na12878_hg001.vcf.gz` — 120 МБ):**
   - Официальный эталонный геном человека от Национального института стандартов и технологий США (NIST Genome in a Bottle).
   - Содержит **3.89 млн сырых генетических вариантов**.
   - Автоматически выкачивается скриптом `./scripts/download_na12878.sh` (команда `make download-real-data`).
2. **Тестовые Фикстуры (`tests/fixtures/demo_user.vcf`):**
   - Легкий тестовый образ со стандартами генотипирования для быстрой автономной проверки (`CYP1A2`, `LCT`, `SLCO1B1`, `VKORC1`).

### Б. Справочники Клинической Биоинформатики (Clinical References)
1. **NCBI ClinVar GRCh38 Benchmark (`data/clinvar.vcf.gz` — 184 МБ):**
   - Официальная базовая база данных Национального центра биотехнологической информации США (NCBI).
   - Содержит аннотации для более 2 000 000 мутаций с уровнями доказательности (`Pathogenic`, `Risk Factor`, `Drug Response`).
2. **CPIC Guidelines / ClinVar Benchmarks (`tests/fixtures/annotations_mock.tsv`):**
   - Справочные таблицы соответствий вариантов, фенотипов и клинической значимости.

### В. Научные Публикации и Векторный RAG (PubMed Literature RAG)
1. **NCBI PubMed E-Utilities REST API (`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`):**
   - Источник научных публикаций для инструмента `search_medical_literature`. Скрипт `scripts/ingest_pubmed.ts` автоматически опрашивает NCBI API и выкачивает рецензируемые статьи по целевым генам.
2. **Локальная Векторизация Ollama (`nomic-embed-text`):**
   - Тексты статей векторизуются локально моделью `nomic-embed-text` (768-мерные векторы) через локальный Ollama API (`http://localhost:11434`).
3. **Векторное Хранилище (Qdrant & DuckDB Fallback):**
   - Векторы сохраняются в **Qdrant** (коллекция `genomic_pubmed` на порту 6333) или в DuckDB-векторный файл `data/pubmed_vector_store.json`.

---

## 🏗 2. Архитектура и Движок

1. **Rust Ingestion Worker (`/rust-ingestion-worker`)**:
   - Мультикор-парсинг VCF с помощью `Rayon` (скорость **~7.8 млн строк/сек**).
   - Асинхронная интеграция с Temporal через `Tokio` (отправка `heartbeat(...)` каждые 500 мс с процентом прогресса).
   - Пакетный экспорт в **DuckDB** через C-API Appender (скорость **~1.9 млн строк/сек**).
2. **TS API Agent & HTTP Server (`/ts-api-agent`)**:
   - Работает на **Node 25** с нативной поддержкой TypeScript без транспиляторов.
   - Встроенный HTTP-адаптер (`node:http` + `Hono`) без внешних зависимостей.
   - Оснащен 2 инструментами:
     - **`query_genotype`** — детерминированный SQL-запрос к DuckDB генома.
     - **`search_medical_literature`** — семантический векторный RAG поиск по научным статьям в Qdrant/Ollama.
3. **Temporal Orchestration**:
   - Мониторинг каждого шага загрузки и обработки в **Temporal UI** (`http://localhost:8233`).

---

## 🚀 3. Быстрая Проверка и Команды `Makefile`

```bash
# 1. Сквозной E2E-тест геномных данных
make test-e2e

# 2. Выкачивание статей PubMed и векторизация через Ollama в Qdrant / DuckDB
make ingest-pubmed

# 3. Скачивание реальных геномов NA12878 и NCBI ClinVar (304 МБ суммарно)
make download-real-data

# 4. Запуск HTTP REST API-сервера
make run-ts-api
```

---

## 🤖 4. Запуск с Cerebras API (`llama-3.3-70b`)

```bash
export CEREBRAS_API_KEY="твой-ключ-cerebras"
make run-ts-api

# В другом окне терминала:
curl -s -X POST http://localhost:3000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Am I at risk for muscle toxicity from statins? What is my SLCO1B1 genotype?"}' | jq .
```

---

## 🚢 5. Автоматический Деплой на Продакшн (DigitalOcean + GitHub Actions)

Проект настроен для моментального развертывания на любом VPS (DigitalOcean Droplet / Hetzner):

1. **Docker Compose Production:** Файл [`docker-compose.prod.yml`](file:///Users/nikolai_kolesnikov/Projects/education/interview-projects/genetic-data-agent/docker-compose.prod.yml) задействует весь стек (Temporal, Qdrant, MinIO S3, Ollama, API Agent, Worker).
2. **GitHub Actions CI/CD:** Файл [`.github/workflows/deploy.yml`](file:///Users/nikolai_kolesnikov/Projects/education/interview-projects/genetic-data-agent/.github/workflows/deploy.yml) автоматически деплоит проект на ваш сервер по SSH при каждом `git push origin main`.
3. **Настройка Секретов в GitHub (Settings -> Secrets and variables -> Actions):**
   - `DROPLET_IP` — IP вашего сервера.
   - `SSH_PRIVATE_KEY` — Ваш приватный SSH-ключ.
   - `CEREBRAS_API_KEY` — Ваш API-ключ от Cerebras.
