# 🧬 Genomic VCF Ingestion & AI Insight Engine

An autonomous, production-ready MVP for high-performance genomic VCF ingestion (Rust multi-threaded Rayon engine), Temporal workflow orchestration, DuckDB SQL joins, and Medical Literature RAG using Ollama (`nomic-embed-text`), Qdrant, and Cerebras LLMs.

---

## 📖 Документация и Гайды

- **[GUIDE.md](GUIDE.md)** — Полное руководство по запуску, архитектуре, командам `Makefile` и деплою.
- **[SPEC.md](SPEC.md)** — Подробная техническая спецификация проекта.

---

## 🗄 Используемые Данные и Источники (Data Sources)

Платформа работает со строгими мировыми биоинформатическими стандартами:

1. **NIST GIAB NA12878 / HG001 Genome (`data/na12878_hg001.vcf.gz` — 120 MB):**
   - Золотой стандарт секвенирования реального генома человека от Национального института стандартов и технологий США (NIST). Содержит 3.89 млн сырых геномных вариантов.
   - Скачивается командой: `make download-real-data` (скрипт `./scripts/download_na12878.sh`).

2. **NCBI ClinVar GRCh38 Benchmark (`data/clinvar.vcf.gz` — 184 MB):**
   - Официальная базовая база данных Национального центра биотехнологической информации США (NCBI). Содержит более 2 000 000 аннотированных мутаций с клинической значимостью (`Pathogenic`, `Risk Factor`, `Drug Response`).

3. **NCBI PubMed Scientific Papers (NCBI E-Utilities API):**
   - Научные публикации для инструмента `search_medical_literature` автоматически выгружаются напрямую из официального API NCBI PubMed (`https://eutils.ncbi.nlm.nih.gov/entrez/eutils/`).
   - Загрузка и векторизация выполняются командой: `make ingest-pubmed`.

---

## ⚡ Быстрый старт (3 основные команды)

```bash
# 1. Запуск сквозного E2E-теста геномных данных
make test-e2e

# 2. Выкачивание научных статей PubMed и их векторизация через Ollama в Qdrant / DuckDB
make ingest-pubmed

# 3. Запуск HTTP REST API-сервера (порт 3000)
make run-ts-api
```

---

## 🧬 Работа с Temporal UI & Rust Worker

```bash
# Вкладка 1: Локальный сервер Temporal (UI на http://localhost:8233)
make temporal-dev

# Вкладка 2: Воркер очереди "genomic-ingestion" (Rust Rayon engine)
make worker

# Вкладка 3: Запуск Workflow ингестии генома
make run-workflow
```

---

## 🤖 Запуск с Cerebras API (Llama-3.3-70B) & PubMed RAG

```bash
export CEREBRAS_API_KEY="твой-ключ-cerebras"
make run-ts-api

# В другом окне (Запрос по геному + научным статьям):
curl -s -X POST http://localhost:3000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What is my SLCO1B1 genotype and statin myopathy risk?"}' | jq .
```
