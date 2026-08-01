# Genomic VCF Ingestion & AI Insight Engine

An autonomous, production-ready MVP for genomic VCF ingestion, hybrid Rust/TS processing, Temporal orchestration, and RAG-based AI bioinformatics insights using DuckDB and Cerebras LLMs.

---

## 📖 Полное руководство пользователя

Подробный пошаговый гайд со всеми командами, настройкой **Cerebras API**, мониторингом в **Temporal UI (`http://localhost:8233`)** и объяснением архитектуры находится в файле:
👉 **[GUIDE.md](file:///Users/nikolai_kolesnikov/Projects/education/interview-projects/genetic-data-agent/GUIDE.md)**

---

## ⚡ Быстрый старт (3 основные команды)

```bash
# 1. Инициализация базы данных DuckDB из тестового VCF
make init-db

# 2. Запуск сквозного интеграционного E2E-теста (без галлюцинаций AI)
make test-e2e

# 3. Запуск HTTP REST API-сервера (на порту 3000)
make run-ts-api
```

---

## 🧬 Работа с Temporal UI (Локальный запуск)

```bash
# Вкладка 1: Локальный сервер Temporal (UI откроется на http://localhost:8233)
make temporal-dev

# Вкладка 2: Воркер очереди "genomic-ingestion" (использует скомпилированный Rust-бинарник)
make worker

# Вкладка 3: Запуск Workflow ингестии
make run-workflow
```

---

## 🤖 Запуск с Cerebras API (Llama-3.3-70B)

```bash
export CEREBRAS_API_KEY="твой-ключ-cerebras"
make run-ts-api

# В другом окне:
curl -X POST http://localhost:3000/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Can I drink coffee? Check my CYP1A2 genetic variant and explain what it means."}'
```
