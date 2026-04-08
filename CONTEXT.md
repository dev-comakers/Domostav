# Domostav AI Write-Off Analysis — Full Context for AI/Developer

> Этот файл содержит всю информацию, чтобы любой AI-ассистент или разработчик мог понять проект, запустить его и продолжить разработку.

---

## Кто, что, зачем

**Project:** AI-платформа для строительной компании Domostav (Чехия), разрабатывается командой Fajnwork/Zinc (Artem Sotsenko - руководитель, Dmitriy Vez - разработчик).

**Клиент:** Олександр Балаж (Domostav) - руководитель, Богдан - технический/строительный менеджер, Олег/Ярослав - инженеры на стройке.

**Проблема:** Domostav не имеет независимого контроля над списанием строительных материалов. Кладовщики считают вручную, руководство не может проверить правильность. AI даёт "второе мнение" — сколько материала _должно было_ уйти на основе выполненных работ, и сравнивает с фактическим остатком. Аномалии = возможное воровство или ошибки.

**Тестовый проект:** Chirana (Mudřany) — крупный строительный объект, начался ~февраль 2026.

**Приоритеты платформы:**

1. **Сопоставление списания материалов** (приоритет #1, реализован MVP)
2. Анализ закупок (будущее)
3. Интеграция фактур от поставщиков в 1С (будущее)

---

## Реализованный MVP — `domostav-ai/`

MVP = Python CLI-скрипт.

### Структура файлов

```
domostav-ai/
├── main.py                          # CLI точка входа (click), 4 шага workflow
├── models.py                        # Pydantic-модели: SPPItem, InventoryItem, WriteoffRecommendation, etc.
├── requirements.txt                 # openpyxl, anthropic, pydantic, rapidfuzz, pyyaml, rich, click
├── config/
│   ├── settings.py                  # API ключи, модель, пороги (15%/30%), waste% и пр.
│   ├── system_prompt.txt            # Общие правила списания для Claude (на английском)
│   └── projects/
│       └── chirana.yaml             # Маппинг колонок и правила для проекта Chirana
├── parsers/
│   ├── spp_parser.py                # Парсинг SPP Excel → list[SPPItem]
│   ├── inventory_parser.py          # Парсинг инвентаризации Excel → list[InventoryItem]
│   ├── nomenclature_parser.py       # Парсинг справочника номенклатуры (14720 позиций)
│   └── mapping_engine.py            # AI-авто-определение структуры неизвестного Excel
├── matching/
│   ├── diameter_extractor.py        # Regex извлечение диаметров из названий (d20, DN20, 20x2.3, průměr 20)
│   ├── category_classifier.py       # Классификация по ключевым словам: PIPE/FITTING/INSULATION/CONSUMABLE/VALVE
│   └── material_matcher.py          # 3-слойный матчинг (article → regex → AI), функция match_all()
├── analysis/
│   ├── writeoff_calculator.py       # Расчёт ожидаемого списания по правилам (waste%, 50/50, etc.)
│   └── anomaly_detector.py          # Сравнение expected vs actual → OK/WARNING/RED_FLAG
├── output/
│   └── excel_generator.py           # Клонирует инвентаризацию, добавляет AI-столбцы + лист "AI Summary"
├── llm/
│   └── client.py                    # Обёртка Claude API: ask(), ask_json(), ask_batch(), трекинг токенов
└── tests/
    └── test_pipeline.py             # Интеграционные тесты на реальных данных Chirana
```

### Workflow (4 шага в main.py)

1. **Загрузка** — парсинг SPP + инвентаризации через openpyxl (read_only=True)
2. **Маппинг** — показать пользователю какие колонки определены, подтвердить Y/n
3. **Анализ** — match_all() (3 слоя) → analyze_all() → recommendations
4. **Результат** — generate_output() → Excel с AI-столбцами

### Запуск

```bash
cd domostav-ai
pip install -r requirements.txt
export ANTHROPIC_API_KEY="your-key-here"

python main.py \
  --spp "../Domostav x Fajnwork/SPP Chirana 02-26.xlsm" \
  --inventory "../Domostav x Fajnwork/Інвентаризація запасів Chirana 02-26/Інвентаризація запасів за групами № NF-30 від 25.02.2026.xlsx" \
  --project chirana

# Без AI (только слои 1+2):
python main.py --spp "..." --inventory "..." --project chirana --no-ai

# Тесты:
PYTHONIOENCODING=utf-8 python tests/test_pipeline.py
```

---

## Данные-источники

Лежат в `../Domostav x Fajnwork/` относительно `domostav-ai/`.

| Файл                                                        | Роль                                           | Что парсим                                                                                                                                                                                                 |
| ----------------------------------------------------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SPP Chirana 02-26.xlsm`                                    | Выполненные работы (SPP)                       | Листы "Fakturace SoD - ZTI" (сантехника) и "Fakturace SoD - ÚT" (отопление). Колонки: I=название, K=ед., L=кол-во, M=цена, N=сумма, R=% за месяц, S=сумма за месяц. Данные с строки 6, заголовки строка 5. |
| `Інвентаризація...NF-30...xlsx`                             | Инвентаризация (вход)                          | 321 позиция. Колонки: B=№, D=артикул, F=название, K=отклонение, N=факт кол-во, Q=по учёту, T=ед., V=цена. Данные с строки 12.                                                                              |
| `Бланк...NF-45...xlsx`                                      | Документ списания (ground truth для валидации) | 225 позиций. Для проверки точности AI-рекомендаций.                                                                                                                                                        |
| `список_номенклатури_по_групам_ВИПРАВЛЕНО.xlsx`             | Справочник материалов                          | 14720 позиций с группами ("0001 TRUBKY PPR" и т.д.)                                                                                                                                                        |
| `SPP_Chirana_02_26_правила_списання_Topeni+Kanalizace.xlsm` | Правила списания (доп. справка)                | 172 строки                                                                                                                                                                                                 |

---

## 3-слойный матчинг (material_matcher.py)

Задача: связать каждую позицию инвентаризации с позициями SPP (выполненными работами).

- **Слой 1 — Article**: точное совпадение артикула (STRE020S4). На практике мало совпадений, т.к. SPP содержит описания работ, а не коды материалов.
- **Слой 2 — Regex**: извлечение (диаметр + категория + тип материала) → сопоставление по фичам. Пример: "Trubka PPR d20" в инвентаризации → (d=20, PIPE, PPR) → ищем в SPP работы с d20 и категорией PIPE. **На данных Chirana: 57.9% (186/321).**
- **Слой 3 — AI**: оставшиеся позиции батчами по 20-25 → Claude API с контекстом SPP. Модель определяет логическую связь. **Ожидаемый доп. охват: 20-30%.**

---

## Правила списания

Определены в `config/system_prompt.txt` (общие) + `config/projects/chirana.yaml` (проектные, приоритетнее).

| Категория                   | Правило                                    | Матчинг                      |
| --------------------------- | ------------------------------------------ | ---------------------------- |
| **Трубы** (PIPE)            | +10% waste (100м работ → 110м списать)     | По диаметру                  |
| **Фитинги** (FITTING)       | Стоимость ≈ стоимость труб (50/50)         | По диаметру, пропорционально |
| **Редукции**                | 50/50 между двумя диаметрами (напр. 20-25) | По обоим диаметрам           |
| **Изоляция** (INSULATION)   | Привязка к трубе по диаметру, +5% waste    | По диаметру трубы            |
| **Расходники** (CONSUMABLE) | Списать в ноль, auto-approve               | Без привязки к SPP           |
| **Вентили** (VALVE)         | 1:1, без waste                             | По диаметру и количеству     |

### Пороги аномалий

- **OK**: отклонение < 15%
- **WARNING**: 15-30%
- **RED FLAG**: > 30% ИЛИ позиция без привязки к SPP

---

## Pydantic-модели (models.py)

- `SPPItem` — строка SPP: row, sheet, name, unit, quantity, price_per_unit, total, percent_month, total_month + extracted: diameter, category, material_type
- `InventoryItem` — строка инвентаризации: row, number, article, name, unit, quantity_fact, quantity_accounting, deviation, price + extracted features
- `NomenclatureItem` — позиция справочника: group, name, unit, article, diameter, category
- `ColumnMapping` — маппинг букв колонок к полям (header_row, data_start_row)
- `MatchResult` — результат матчинга: inventory_row, matched_spp_rows[], match_method, confidence, match_reason
- `WriteoffRecommendation` — финальная рекомендация: expected_writeoff, actual_deviation, spp_reference, reason, status, deviation_percent

**Enums:**

- `MaterialCategory`: PIPE, FITTING, INSULATION, CONSUMABLE, VALVE, OTHER
- `MatchMethod`: ARTICLE, REGEX, AI, MANUAL, UNMATCHED
- `AnomalyStatus`: OK, WARNING, RED_FLAG

---

## Выходной Excel (output/excel_generator.py)

Клонирует файл инвентаризации, добавляет 6 столбцов справа:

| Столбец            | Содержимое                                    |
| ------------------ | --------------------------------------------- |
| AI: Ожид. списание | Расчётное кол-во для списания (число)         |
| AI: Привязка к SPP | Какие позиции SPP соответствуют (текст)       |
| AI: Причина        | Объяснение расчёта (текст)                    |
| AI: Статус         | OK / WARNING / RED_FLAG (с цветовой заливкой) |
| AI: Метод          | ARTICLE / REGEX / AI / MANUAL / UNMATCHED     |
| AI: Откл. %        | Процент отклонения ожидаемого от фактического |

Плюс лист **"AI Summary"** со сводкой и топ-аномалиями.

---

## Результаты тестового запуска (Chirana, без AI)

- SPP: **457 позиций** (ZTI + ÚT)
- Инвентаризация: **321 позиция**
- Regex-матчинг (слой 2): **186 совпадений (57.9%)**
- Без AI: 8 OK, 2 WARNING, 311 RED FLAG
- С AI (слой 3) ожидается значительное улучшение

---

## Технические нюансы

- **Windows:** Rich console крашится без `PYTHONIOENCODING=utf-8` из-за кодировки cp1252
- **openpyxl read_only=True:** EmptyCell объекты не имеют `.row` — в парсерах есть try/except с fallback на счётчик
- **Claude API:** модель по умолчанию `claude-sonnet-4-20250514`, батчинг по 25 позиций, cost tracking в `ClaudeClient.get_usage_summary()`
- **Чешская терминология:** названия на чешском/словацком с диакритикой (ů, ř, č, í). Regex для диаметров: d20, DN20, průměr 20, prumner 20, 20x2.3
- **SPP .xlsm** (макро-книга) — openpyxl читает data_only=True, макросы игнорируются

---

## Статус MVP (на 2026-03-18)

**Реализовано:**

- CLI pipeline с 4-шаговым workflow
- Все парсеры (SPP, инвентаризация, номенклатура)
- AI-автодетект маппинга колонок
- 3-слойный матчинг (article → regex → AI)
- Расчёт ожидаемого списания
- Определение аномалий (OK/WARNING/RED FLAG)
- Генерация Excel с AI-столбцами и цветовой разметкой
- Claude API обёртка с батчингом
- Конфиг Chirana
- Интеграционные тесты — **все 5 проходят**

**Что НЕ реализовано / следующие шаги:**

- Веб-платформа (сейчас только CLI)
- Валидация AI-рекомендаций против документа списания NF-45 (225 позиций ground truth)
- Парсинг файла правил списания (`SPP_Chirana_02_26_правила_списання_Topeni+Kanalizace.xlsm`)
- Модуль анализа закупок (приоритет #2)
- Интеграция с 1С / фактурами поставщиков (приоритет #3)
- Поддержка нескольких проектов одновременно

---

## Как добавить новый проект

1. Создать `config/projects/<name>.yaml` по образцу `chirana.yaml`
2. Указать маппинг колонок для SPP и инвентаризации
3. Указать правила списания (если отличаются от общих)
4. Запустить: `python main.py --spp "..." --inventory "..." --project <name>`
