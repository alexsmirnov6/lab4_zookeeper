"""Build the ZooKeeper report from recorded experiments and captured screenshots."""

import argparse
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape

from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"
FIGURES = ROOT / "report/figures"
RUNS = ("fencing-0-20261030T102741-190900", "fencing-1-20261030T102900-401400")


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_events(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def count(value):
    return f"{value:,}".replace(",", " ")


def seconds(value):
    return f"{value:.3f}".replace(".", ",")


def command_block(log, prefix):
    lines = log.splitlines()
    start = next(i for i, line in enumerate(lines) if line.startswith(prefix))
    end = next((i for i in range(start + 1, len(lines)) if lines[i].startswith("$ ")), len(lines))
    return "\n".join(lines[start:end]).strip()


def publication_rows(events):
    return [event for event in events if event["event"] in ("записал результат", "publication_rejected")]


def verify_evidence():
    ensemble = read_json(LOGS / "ensemble/summary.json")
    reference = read_json(ROOT / "reference/lab3.json")
    summaries = [read_json(LOGS / run / "summary.json") for run in RUNS]
    receivers = [read_events(LOGS / run / "receiver.jsonl") for run in RUNS]
    for roles in (ensemble["initial_roles"], ensemble["restored_roles"]):
        if sorted(roles.values()) != ["follower", "follower", "leader"]:
            raise ValueError("The ensemble roles are incomplete")
    for mode, (summary, events) in enumerate(zip(summaries, receivers)):
        accepted = [event for event in events if event["event"] == "записал результат"]
        rejected = [event for event in events if event["event"] == "publication_rejected"]
        if not summary["all_passed"] or len(accepted) != 2 - mode or len(rejected) != mode:
            raise ValueError("The publication log and experiment summary do not agree")
        if summary["pointer_version"] != 2 - mode or summary["requested_session_timeout_seconds"] != 10:
            raise ValueError("Unexpected pointer version or session timeout")
        if [event["version"] for event in accepted] != list(range(1, 3 - mode)):
            raise ValueError("Unexpected publication versions in the receiver log")
        owner = summary["backup"] if mode else summary["leader"]
        if summary["pointer"]["coordinator"] != owner or summary["pointer"]["epoch"] != mode + 1:
            raise ValueError("The final pointer has the wrong owner or epoch")
        if mode and (rejected[0]["error"] != "BadVersionError" or rejected[0]["coordinator"] != summary["leader"]):
            raise ValueError("The stale coordinator was not rejected by epoch")
        if set(summary["result_sha256"]) != {summary["leader"], summary["backup"]}:
            raise ValueError("One of the coordinator results is missing")
        if any(value != reference["pairs_canonical_sha256"] for value in summary["result_sha256"].values()):
            raise ValueError("The result SHA-256 differs from lab3")
        if summary["input"]["sha256"] != reference["input_sha256"]:
            raise ValueError("The input differs from lab3")
        backup_events = read_events(LOGS / RUNS[mode] / f"{summary['backup']}.jsonl")
        loaded = next(event for event in backup_events if event["event"] == "checkpoint_loaded")
        if (loaded["completed"], loaded["vacancies"]) != (summary["checkpoint_chunks_loaded"], summary["checkpoint_vacancies_loaded"]):
            raise ValueError("The checkpoint log and summary do not agree")
        if not 0 < loaded["completed"] < 29:
            raise ValueError("The experiment must recover from a partial checkpoint")
    if not (ensemble["all_passed"] and ensemble["ephemeral"]["absent_after_quit"]
            and ensemble["one_server_stopped"]["write_read_succeeded"]
            and ensemble["two_servers_stopped"]["read_write_session_unavailable"]):
        raise ValueError("The ensemble checks did not pass")
    if "not currently serving requests" not in (LOGS / "ensemble/no-quorum.txt").read_text(encoding="utf-8"):
        raise ValueError("The no-quorum server response is missing")
    return ensemble, reference, summaries, receivers


def build(font_dir, destination):
    ensemble, reference, summaries, receivers = verify_evidence()
    for name, filename in (("Report", "times.ttf"), ("Report-Bold", "timesbd.ttf"),
                           ("Report-Italic", "timesi.ttf"), ("Report-Code", "consola.ttf")):
        pdfmetrics.registerFont(TTFont(name, str(font_dir / filename)))
    styles = {
        "body": ParagraphStyle("Body", fontName="Report", fontSize=11, leading=15.5, spaceAfter=8),
        "small": ParagraphStyle("Small", fontName="Report", fontSize=9, leading=12, spaceAfter=6),
        "h1": ParagraphStyle("Heading", fontName="Report-Bold", fontSize=16, leading=21, spaceAfter=13),
        "h2": ParagraphStyle("Subheading", fontName="Report-Bold", fontSize=12, leading=16, spaceBefore=8, spaceAfter=7),
        "title": ParagraphStyle("Title", fontName="Report-Bold", fontSize=20, leading=26, alignment=TA_CENTER, spaceAfter=14),
        "center": ParagraphStyle("Center", fontName="Report", fontSize=12, leading=17, alignment=TA_CENTER, spaceAfter=7),
        "code": ParagraphStyle("Code", fontName="Report-Code", fontSize=7.8, leading=10.2, spaceAfter=9),
    }
    width = A4[0] - 40 * mm
    story = []

    def p(value, style="body"):
        return Paragraph(escape(str(value)).replace("\n", "<br/>"), styles[style])

    def code(value):
        return Preformatted(value, styles["code"], maxLineLength=99, splitChars=" ", newLineChars="  ")

    def table(headers, rows, widths, size=9):
        cell = ParagraphStyle("Cell", fontName="Report", fontSize=size, leading=size + 3)
        header = ParagraphStyle("HeaderCell", parent=cell, fontName="Report-Bold")
        data = [[Paragraph(escape(str(value)), header) for value in headers]]
        data += [[Paragraph(escape(str(value)), cell) for value in row] for row in rows]
        result = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
        result.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LINEBELOW", (0, 0), (-1, 0), .8, colors.black),
            ("LINEBELOW", (0, 1), (-1, -1), .35, colors.grey),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))
        return result

    def heading(title):
        story.append(p(title, "h1"))

    def page():
        story.append(PageBreak())

    def figure(name, caption):
        image = Image(str(FIGURES / name))
        ratio = min(width / image.imageWidth, 270 / image.imageHeight)
        image.drawWidth = image.imageWidth * ratio
        image.drawHeight = image.imageHeight * ratio
        story.extend([image, Spacer(1, 5), p(caption, "small")])

    def event_excerpt(event):
        selected = {key: event[key] for key in ("time", "event", "coordinator", "epoch", "version", "error") if key in event}
        return json.dumps(selected, ensure_ascii=False, indent=2)

    def timeline(mode):
        folder = LOGS / RUNS[mode]
        rows = []
        for who in ("coordinator-a", "coordinator-b"):
            for event in read_events(folder / f"{who}.jsonl"):
                if event["event"] == "became_leader":
                    rows.append([event["time"][11:23], who, f"Получил лидерство, эпоха {event['epoch']}"])
                elif event["event"] == "checkpoint_loaded":
                    rows.append([event["time"][11:23], who, f"Загрузил {event['completed']} чанк, {event['vacancies']} вакансий"])
        for event in publication_rows(receivers[mode]):
            result = (f"Записал результат, версия {event['version']}" if "version" in event else event["error"])
            rows.append([event["time"][11:23], event["coordinator"], result])
        return sorted(rows)

    story += [Spacer(1, 22 * mm), p("Домашнее задание № 4", "center"),
              p("Обеспечение отказоустойчивости\nсредствами Apache ZooKeeper", "title"),
              p("Расчёт пар навыков в вакансиях DevOps", "center"), Spacer(1, 9 * mm),
              p("Смирнов Александр, группа J3310", "center"),
              p("30 октября 2026 года", "center"), Spacer(1, 12 * mm),
              p("Цель работы", "h2"),
              p("Развернуть ансамбль ZooKeeper из трёх серверов и проверить кворум и эфемерные "
                "узлы. Интегрировать его с расчётом пар навыков из ДЗ № 3: выбрать активный "
                "координатор, восстановить расчёт после его приостановки и сравнить публикацию "
                "результата без защиты и с проверкой эпохи."),
              p("Основные результаты", "h2"),
              table(["Проверка", "Результат"], [
                  ["Ансамбль", "Один лидер и два последователя; один отказ допустим, два приводят к потере кворума"],
                  ["Эфемерный узел", "Создан через zkCli и удалён после закрытия сессии"],
                  ["Восстановление", "Запасной загрузил 1 готовый чанк с 218 вакансиями и обработал оставшиеся 28 чанков"],
                  ["Публикация", "Без защиты приняты две записи; с защитой старая эпоха отклонена с BadVersionError"],
                  ["Корректность", "Все четыре полных результата совпали с эталоном ДЗ № 3 по SHA-256"],
              ], [125, width - 125])]
    page()

    heading("Стенд и конфигурация")
    story += [p("Стенд запущен в Docker Desktop с Linux-контейнерами. Использован официальный "
                "образ ZooKeeper 3.9.6 с Java 17. Каждый сервер имеет отдельные тома /data "
                "и /datalog; одинаковые пути внутри контейнеров изолированы. Общая конфигурация "
                "config/zoo.cfg задаёт один и тот же состав ансамбля, а ZOO_MY_ID создаёт "
                "разный файл /data/myid."),
              table(["Сервер", "myid", "Клиентский порт", "Начальная роль"], [
                  [name, index, f"127.0.0.1:{2180 + index}", role]
                  for index, (name, role) in enumerate(ensemble["initial_roles"].items(), 1)
              ], [65, 45, 165, width - 275]), Spacer(1, 8),
              code("tickTime=2000\ninitLimit=10\nsyncLimit=5\ndataDir=/data\ndataLogDir=/datalog\n"
                   "clientPort=2181\nminSessionTimeout=4000\nmaxSessionTimeout=20000\n"
                   "standaloneEnabled=false\nserver.1=zk1:2888:3888\nserver.2=zk2:2888:3888\nserver.3=zk3:2888:3888"),
              p("Фрагмент zoo.cfg. Порты 2888 и 3888 используются внутри сети Compose. "
                "Клиентские порты опубликованы только на localhost.", "small")]
    figure("docker-ensemble.png", "Рисунок 1. Три контейнера ZooKeeper 3.9.6 и отдельный RustFS. "
           "Снимок показывает состояние стенда после экспериментов; роли подтверждены ответами srvr.")
    story += [p("Для приложения используются два координатора и отдельный получатель публикаций. "
                "RustFS ДЗ № 4 доступен на портах 9100 и 9101 и не пересекается с хранилищем ДЗ № 3.")]
    page()

    heading("Проверка кворума и эфемерного узла")
    ensemble_log = (LOGS / "ensemble/commands.log").read_text(encoding="utf-8")
    story += [code("python scripts/check_ensemble.py"),
              p("Сценарий определяет роли через TCP-команду srvr, затем проверяет реальную "
                "запись и чтение контрольного значения. В начальных ответах zk1 и zk2 "
                "указано Mode: follower, у zk3 - Mode: leader."),
              table(["Состояние", "Наблюдение"], [
                  ["Работают все три сервера", "Один лидер, два последователя; запись и чтение успешны"],
                  ["Остановлен zk1", "Запись нового значения и чтение через оставшийся кворум успешны"],
                  ["Остановлены zk1 и zk2", "Read-write сессия не устанавливается; ответ zk3 приведён ниже"],
                  ["zk1 и zk2 возвращены", "Снова один лидер и два последователя; запись и чтение восстановлены"],
              ], [150, width - 150]), Spacer(1, 8),
              code(command_block(ensemble_log, "$ docker compose stop zk2")),
              code((LOGS / "ensemble/no-quorum.txt").read_text(encoding="utf-8").strip()),
              p("Контейнер zk3 продолжает работать, но без большинства не обслуживает запросы. "
                "Проверка не разрешает read-only соединение; ожидание обычной сессии "
                "завершилось по тайм-ауту 5 секунд."),
              p("Эфемерный узел", "h2")]
    ephemeral = ensemble["ephemeral"]
    cli_log = (LOGS / "ensemble/zkcli-ephemeral.log").read_text(encoding="utf-8")
    cli_lines = [line for line in cli_log.splitlines() if line.startswith(("$ ", "> "))
                 or "Created /lab4/ensemble/" in line or "state:Closed" in line]
    story += [p("Узел создан штатным zkCli.sh. Независимый клиент Kazoo подтвердил его "
                "наличие и ненулевой ephemeralOwner, затем дождался исчезновения после quit."),
              code("\n".join(cli_lines)),
              table(["Проверка наблюдателя", "Значение из summary.json"], [
                  ["Владелец сессии", hex(ephemeral["owner"])],
                  ["Узел отсутствует после quit", str(ephemeral["absent_after_quit"]).lower()],
                  ["Все проверки ансамбля выполнены", str(ensemble["all_passed"]).lower()],
              ], [230, width - 230]), Spacer(1, 7),
              p("Эфемерный узел связан с сессией. При quit она закрывается явно; "
                "при зависании процесса узел удаляется после истечения сессии. "
                "Именно истечение сессии далее освобождает лидерство координатора.", "small")]
    page()

    heading("Координатор и восстановление из чекпоинта")
    story += [p("Оба координатора используют один NDJSON-сегмент ДЗ № 3: 6 215 вакансий, "
                "1 880 228 байт. Сохранены правила S3 Range GET, нормализация навыков "
                "и формат pairs.json. Два Mapper-процесса считают пары по чанкам размером "
                "65 536 байт, координатор объединяет частоты."),
              table(["Состояние", "Место и назначение"], [
                  ["election/candidate-...", "Эфемерные последовательные узлы; минимальный номер выбирает лидера"],
                  ["epoch", "Версия полномочий; повышается новым лидером"],
                  ["checkpoint", "Указатель на S3-объект со счётчиками, числом вакансий и ID готовых чанков"],
                  ["result, publications", "Указатель полного результата и журнал принятых публикаций"],
              ], [155, width - 155]), Spacer(1, 8),
              p("Запасной проверяет SHA-256 чекпоинта, идентификатор входа и размер чанка, "
                "восстанавливает Counter и отправляет Mapper только оставшиеся задачи. "
                "Готовые чанки не учитываются повторно. В обоих опытах журнал запасного "
                "содержит checkpoint_loaded: completed=1, vacancies=218. Он продолжил "
                "расчёт с 28 оставшихся чанков, а не начал с нуля."),
              p("ZooKeeper хранит небольшие указатели, крупные счётчики остаются в RustFS. "
                "Получатель публикаций работает с собственной активной сессией. "
                "Проверка эпохи чекпоинта включена в обоих режимах; FENCING меняет только "
                "защиту финальной публикации.")]
    figure("docker-service-logs.png", "Рисунок 2. Проект lab4-zookeeper и текущие журналы в Docker Desktop. "
           "Координаторы и получатель после опытов остановлены. События экспериментов "
           "подтверждены сохранёнными журналами в следующих разделах.")
    page()

    heading("Приостановка лидера без защиты")
    summary = summaries[0]
    story += [code("python scripts/run_failover.py --fencing 0"),
              p("После первого чекпоинта coordinator-a приостановлен целиком через docker "
                "compose pause минимум на 40 секунд. Тайм-аут сессии равен 10 секундам. "
                "После её истечения coordinator-b получил эпоху 2, восстановил чекпоинт "
                "и опубликовал результат. Старый лидер возобновлён после этой публикации."),
              table(["Время журнала, UTC", "Процесс", "Событие"], timeline(0),
                    [115, 115, width - 230], size=8.5), Spacer(1, 8),
              p("Две записи «записал результат»", "h2"),
              p("Из receiver.jsonl выбраны события публикации; поля key и component опущены.", "small")]
    for record in publication_rows(receivers[0]):
        story.append(code(event_excerpt(record)))
    story += [table(["Итог", "Значение"], [
                  ["Время до обнаружения запасного лидером", seconds(summary["leadership_change_seconds"]) + " с"],
                  ["Принято публикаций / версия result", f"{summary['accepted_publications']} / {summary['pointer_version']}"],
                  ["Итоговый указатель", f"{summary['pointer']['coordinator']}, эпоха {summary['pointer']['epoch']}"],
              ], [260, width - 260]), Spacer(1, 7),
              p("Старый лидер завершил вычисление и был принят вторым с устаревшей эпохой 1. "
                "Численные результаты совпали, но двойная публикация состоялась. "
                "Для приложения с побочными эффектами она могла бы иметь последствия.")]
    page()

    heading("Приостановка лидера с защитой по эпохе")
    summary = summaries[1]
    story += [code("python scripts/run_failover.py --fencing 1"),
              p("Опыт повторён с тем же входом и параметрами в отдельном пространстве "
                "ZooKeeper и префиксе S3. Получатель проверяет текущую эпоху и меняет "
                "указатель одной транзакцией. Фрагмент src/receiver.py:"),
              code("transaction = zk.transaction()\ntransaction.check(root + '/epoch', version=request['epoch'])\n"
                   "transaction.set_data(root + '/result', encode(request), version=-1)\n"
                   "transaction.create(root + '/publications/entry-',\n                   encode(request), sequence=True)\n"
                   "commit_checked(transaction)"),
              table(["Время журнала, UTC", "Процесс", "Событие"], timeline(1),
                    [115, 115, width - 230], size=8.5), Spacer(1, 8),
              p("Принятая и отклонённая публикации", "h2")]
    for record in publication_rows(receivers[1]):
        story.append(code(event_excerpt(record)))
    story += [table(["Итог", "Значение"], [
                  ["Время до обнаружения запасного лидером", seconds(summary["leadership_change_seconds"]) + " с"],
                  ["Принято / отклонено публикаций", f"{summary['accepted_publications']} / {summary['rejected_publications']}"],
                  ["Версия и владелец result", f"{summary['pointer_version']}; {summary['pointer']['coordinator']}, эпоха {summary['pointer']['epoch']}"],
              ], [260, width - 260]), Spacer(1, 7),
              p("BadVersionError не даёт старому координатору изменить указатель. В S3 "
                "может остаться его неизменяемая заготовка, но опубликованным считается "
                "принятый result и запись publications: с защитой такая публикация одна.", "small")]
    page()

    heading("Проверка результатов и выводы")
    story += [table(["Показатель", "Без защиты", "С защитой"], [
                  ["Время смены лидера", seconds(summaries[0]["leadership_change_seconds"]) + " с", seconds(summaries[1]["leadership_change_seconds"]) + " с"],
                  ["Загружено из чекпоинта", "1 чанк, 218 вакансий", "1 чанк, 218 вакансий"],
                  ["Принято публикаций / версия result", "2 / 2", "1 / 1"],
                  ["Устаревшая публикация", "Принята", "BadVersionError"],
                  ["Полное совпадение с ДЗ № 3", "Оба координатора", "Оба координатора"],
              ], [210, (width - 210) / 2, (width - 210) / 2]), Spacer(1, 8),
              p(f"Во всех четырёх результатах {count(reference['vacancies'])} вакансий, "
                f"{count(reference['unique_pairs'])} уникальные пары и {count(reference['total_pairs'])} "
                "вхождения. Совпал SHA-256 полного канонического JSON, включая метаданные и все частоты:"),
              code(reference["pairs_canonical_sha256"]),
              p("Как измерено время", "h2"),
              p("Используются монотонные часы управляющего скрипта: от подтверждения pause "
                "до обнаружения новой эпохи, с опросом каждые 0,1 секунды. Отметки UTC "
                "Windows и контейнеров не вычитаются друг из друга. Интервал включает "
                "задержку наблюдения и зависит от последнего heartbeat и момента истечения "
                "сессии; тайм-аут 10 секунд не означает ровно 10 секунд после pause."),
              p("Почему три сервера, а не два или четыре", "h2"),
              p("Для двух серверов большинство равно двум: один отказ останавливает ансамбль. "
                "У трёх нужны два голоса, поэтому один отказ допустим. У четырёх нужны три, "
                "и допустим всё тот же один отказ. Три сервера дают нужную устойчивость "
                "без четвёртого участника, который её не увеличивает; для двух отказов нужны пять."),
              p("Почему эпоху проверяет получатель", "h2"),
              p("ZooKeeper завершает сессию, но не может остановить старый процесс и отменить "
                "его запрос к другому сервису. Поэтому получатель должен отклонить запись "
                "с устаревшими полномочиями. В нашей реализации приложение требует проверки "
                "версии, а ZooKeeper атомарно выполняет её вместе с обновлением указателя. "
                "Отдельная проверка до записи оставила бы промежуток для смены лидера."),
              p("Кворум сохраняет работоспособность координации при одном отказе; чекпоинт "
                "сохраняет выполненную часть расчёта; проверка эпохи защищает публикацию "
                "после возвращения старого лидера. Это три дополняющих друг друга механизма.")]
    page()

    heading("Повторение опыта и материалы")
    story += [code("pip install -r requirements.txt\ndocker compose up -d --wait\n"
                   "python scripts/check_ensemble.py\npython scripts/prepare_data.py\n"
                   "python -m unittest discover -s tests -v\n"
                   "python scripts/run_failover.py --fencing 0\npython scripts/run_failover.py --fencing 1"),
              p("Команды выполняются в Python-окружении проекта. prepare_data.py берёт "
                "NDJSON из соседнего lab3; другой путь задаётся через --source. Коммит "
                "и хеш входа закреплены в reference/lab3.json. Каждый опыт создаёт свой run_id."),
              table(["Материал", "Подтверждение"], [
                  ["compose.yaml; config/zoo.cfg", "Три участника, myid и раздельное хранение"],
                  ["logs/ensemble/", "Роли, потеря одного и двух серверов, создание и исчезновение эфемерного узла"],
                  [RUNS[0], "Две публикации, указатель версии 2 и измерение смены лидера"],
                  [RUNS[1], "BadVersionError, указатель запасного версии 1 и измерение смены лидера"],
                  ["summary.json обоих запусков", "Чекпоинт, четыре хеша результатов и all_passed=true"],
              ], [200, width - 200], size=8.5), Spacer(1, 8),
              p("Каталоги запусков находятся в logs/. В каждом сохранены coordinator-a.jsonl, "
                "coordinator-b.jsonl, receiver.jsonl, commands.log и summary.json. "
                "Снимки находятся в report/figures/. Полные журналы не заменены картинками.", "small"),
              p("Границы проверки", "h2"),
              p("Все контейнеры размещены на одном компьютере: проверены отказы процессов, "
                "а не независимых машин. Каждый режим выполнен одним полным запуском. "
                "Отказ RustFS и самого получателя, повреждение диска и обрыв ответа "
                "после успешной записи в этих опытах не моделировались."),
              p("Источники", "h2"),
              p("Код и журналы: https://github.com/alexsmirnov6/lab4_zookeeper\n"
                "Эталон ДЗ № 3: https://github.com/alexsmirnov6/lab3_devops", "small"),
              p("Apache ZooKeeper 3.9.6: конфигурация и кворум.\n"
                "https://zookeeper.apache.org/doc/r3.9.6/zookeeperAdmin.html", "small"),
              p("Apache ZooKeeper 3.9.6: сессии, эфемерные узлы и версии.\n"
                "https://zookeeper.apache.org/doc/r3.9.6/zookeeperProgrammers.html", "small"),
              p("Образ: https://hub.docker.com/_/zookeeper\n"
                "Клиент и транзакции: https://kazoo.readthedocs.io/en/latest/api/client.html", "small")]

    destination.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(str(destination), pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm,
                                 topMargin=19 * mm, bottomMargin=19 * mm,
                                 title="Обеспечение отказоустойчивости средствами Apache ZooKeeper",
                                 author="Смирнов Александр", subject="Домашнее задание 4: ансамбль, failover и fencing")
    document.build(story)
    pages = [page.extract_text() or "" for page in PdfReader(destination).pages]
    required = ["Эфемерный узел", "Приостановка лидера без защиты", "Приостановка лидера с защитой",
                "9,717", "10,320", "BadVersionError", "173 204", "25 462", "Источники"]
    text = "\n".join(pages)
    missing = [item for item in required if item.casefold() not in text.casefold()]
    if missing or "Источники" not in pages[-1] or "Paragraph(" in text:
        raise ValueError(f"Report content check failed: {missing}")
    quality = {"file": destination.relative_to(ROOT).as_posix(), "pages": len(pages),
               "evidence_consistent": True, "required_content_present": True,
               "sources_on_final_page": True, "screenshots": 2,
               "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}
    (ROOT / "report/quality.json").write_text(json.dumps(quality, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(quality, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--font-dir", type=Path, default=Path("C:/Windows/Fonts"))
    parser.add_argument("--out", type=Path, default=ROOT / "report/lab4_zookeeper.pdf")
    arguments = parser.parse_args()
    build(arguments.font_dir, arguments.out)
