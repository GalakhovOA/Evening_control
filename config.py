# =============================
# РОЛИ / СПРАВОЧНИКИ
# =============================

# Список ФИ для РТП (без ID)
RTP_LIST = [
    "Чепик Ольга",
    "Матвеева Анастасия",
    "Ионов Александр",
    "Туманцева Ольга",
    "Ворфоломеева Ольга",
    "Самойлова Татьяна"
]

# Список ФИ для РМ/МН (без ID)
RM_MN_LIST = [
    "Региональный менеджер",
    "Менеджер направления"
]

# Пароль администратора
ADMIN_PASSWORD = "СРБ"

# Пароль для входа РТП (редактируется админом в панели)
RTP_PASSWORD = "СРБ"

# Варианты продуктов для ФЦКП
FCKP_OPTIONS = ["ТЭ", "ЗП", "БК", "БГ", "РКО"]

# =============================
# ВОПРОСЫ ОТЧЁТА (МКК)
# =============================

QUESTIONS = [
    {"key": "meetings", "question": "1. Встречи - (шт):"},
    {"key": "meetings_ca", "question": "2. Встречи ЦА - (шт):"},
    {"key": "meetings_stars", "question": "3. Встречи 0-2 звезды - (шт):"},
    {"key": "meetings_recorded", "question": "4. Запись встреч - (шт):"},
    {"key": "knk_opened", "question": "5. Открыто КНК - (шт):"},
    {"key": "fckp_realized", "question": "6. Реализовано ФЦКП - (шт):"},
    {"key": "leasing_leads", "question": "7. Лизинг передано лидов - (шт):"},
    {"key": "credit_potential", "question": "8. Расчет кредитного потенциала - (шт):"},
    {"key": "credits_issued_mln", "question": "9. Кредитов заведено - (млн):"},
    {"key": "otr", "question": "10. ОТР - (шт):"},
    {"key": "pu", "question": "11. ПУ - (шт):"},
    {"key": "chats", "question": "12. Чатов - (шт):"},
    {"key": "calls", "question": "13. Количество звонков - (шт):"},
    {"key": "new_recipients", "question": "14. Количество новых получателей ЗП - (шт):"},
    {"key": "callbacks", "question": "15. Обратные звонки - (шт):"}
]

# =============================
# Опер. дефекты (РТП)
# =============================

OPERATIONAL_DEFECTS_BLOCK = """
Опер.дефекты
1. Отрицательные заключение - нет шт.
2. Выход из МФ - 0 шт.
3. ИП с ограничениями - 0 шт.
4. Передача досье кредиты , ЗП , ТЭ - 0 шт.
5. Кредитные сделки на 1 стадии до 5 дней - 0 шт.
6. Наличие комментариев по встречам
7. Сформирована Повестка БУ-0
"""

# =============================
# ФОРМАТИРОВАНИЕ
# =============================

def format_value(v):
    try:
        if v is None or v == "":
            return "0"
        if isinstance(v, (int, float)):
            if float(v).is_integer():
                return str(int(v))
            return f"{float(v):.2f}".rstrip("0").rstrip(".")
        if isinstance(v, str):
            f = float(v.replace(",", ".").strip())
            if f.is_integer():
                return str(int(f))
            return f"{f:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(v)

def calc_percent(part, total):
    try:
        part = float(part)
        total = float(total)
        if total == 0:
            return "0%"
        return f"{round((part / total) * 100)}%"
    except Exception:
        return "0%"

# =============================
# ФОРМИРОВАНИЕ ОТЧЁТА
# =============================

def format_report(data):
    lines = []
    lines.append("Производительность")

    meetings = data.get("meetings", 0)
    meetings_recorded = data.get("meetings_recorded", 0)
    credit_potential = data.get("credit_potential", 0)

    meetings_recorded_percent = calc_percent(meetings_recorded, meetings)
    credit_percent = calc_percent(credit_potential, meetings)

    for q in QUESTIONS:
        val = data.get(q["key"], 0)

        if q["key"] == "meetings_recorded":
            lines.append(
                f"{q['question']} {format_value(val)} ({meetings_recorded_percent})"
            )
        elif q["key"] == "credit_potential":
            lines.append(
                f"{q['question']} {format_value(val)} ({credit_percent})"
            )
        else:
            lines.append(f"{q['question']} {format_value(val)}")

    # ФЦКП
    lines.append("")
    lines.append("ФЦКП (детализация):")

    prod_counts = {}
    for p in data.get("fckp_products", []):
        prod_counts[p] = prod_counts.get(p, 0) + 1

    for opt in FCKP_OPTIONS:
        lines.append(f"{opt} - {format_value(prod_counts.get(opt, 0))} шт")

    return "\n".join(lines)
