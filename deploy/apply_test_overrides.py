import argparse
import shutil
import sys
from pathlib import Path

OVERRIDES = Path(__file__).resolve().parent / "test_overrides"

# Полная замена файла — для конфигов, где на тесте нужен
# полностью свой набор значений. Правится редактированием файла
# в test_overrides/, этот скрипт не трогается.
FULL_COPY = [
    ("config/module_ch_api_gateway.yaml", "src/project/config/module_ch_api_gateway.yaml"),
    ("config/module_ch_loader.yaml", "src/project/config/module_ch_loader.yaml"),
    ("config/module_reputation.yaml", "src/project/config/module_reputation.yaml"),
    ("config/module_data_collector.yaml", "src/project/config/module_data_collector.yaml"),
    ("config/dg_sources.yaml", "src/project/config/dg_sources.yaml"),
]

# Точечные правки внутри файлов, которые в остальном должны остаться
# боевыми — там код, а не только данные. Каждый патч содержит и "prod",
# и "test" вид строки: если найден "test" — уже применено, пропускаем;
# если найден "prod" — патчим; если не найдено ни то, ни другое — файл
# поменялся в разработке, останавливаемся и просим обновить рецепт.
LINE_PATCHES = [
    {
        "file": "src/project/module_data_collector/main.py",
        "prod": "        ab = AbProducer(nc, config, lifecycle)\n",
        "test": "        # ab = AbProducer(nc, config, lifecycle)  # <- закомментировано скриптом для теста\n",
    },
    {
        "file": "src/project/module_data_collector/main.py",
        "prod": "            asyncio.create_task(ab.start()),\n",
        "test": "            # asyncio.create_task(ab.start()),  # <- закомментировано скриптом для теста\n",
    },
    {
        "file": "src/project/module_reputation/infrastructure/ch_client.py",
        "prod": "              INSERT INTO feedgen.ip_reputation_snapshots_data\n",
        "test": "              INSERT INTO feedgen.ip_reputation_snapshots\n",
    },
    {
        "file": "src/project/module_ch_api_gateway/infrastructure/feed_list_mirror_client.py",
        "prod": "              INSERT INTO feedgen.feed_list_mirror_data\n",
        "test": "              INSERT INTO feedgen.feed_list_mirror\n",
    },
    {
        "file": "src/project/module_ch_api_gateway/infrastructure/feed_list_mirror_client.py",
        "prod": '    "ALTER TABLE feedgen.feed_list_mirror_data "\n',
        "test": '    "ALTER TABLE feedgen.feed_list_mirror "\n',
    },
    {
        "file": "src/project/module_data_collector/main.py",
        "prod": (
            "        try:\n"
            "            if config.get(\"targeted_ab_client\", {}).get(\"url\"):\n"
            "                targeted = TargetedAbProducer(nc, config, lifecycle)\n"
            "                tasks.append(asyncio.create_task(targeted.start()))\n"
            "            else:\n"
            "                logger.info(\"action=targeted_ipban_skipped reason=not_configured\")\n"
            "        except Exception as e:\n"
            "            logger.error(\"action=targeted_ipban_init_failed error=%s\", str(e))\n"
        ),
        "test": (
            "        # try:  # <- закомментировано скриптом для теста (новый ipban-источник отключён)\n"
            "        #     if config.get(\"targeted_ab_client\", {}).get(\"url\"):\n"
            "        #         targeted = TargetedAbProducer(nc, config, lifecycle)\n"
            "        #         tasks.append(asyncio.create_task(targeted.start()))\n"
            "        #     else:\n"
            "        #         logger.info(\"action=targeted_ipban_skipped reason=not_configured\")\n"
            "        # except Exception as e:\n"
            "        #     logger.error(\"action=targeted_ipban_init_failed error=%s\", str(e))\n"
        ),
    },
    {
        "file": "front/scripts/auth.js",
        "prod": 'export const API_BASE = "http://10.25.86.13:8000";\n',
        "test": 'export const API_BASE = "http://192.168.100.113:8001";\n',
    },
]


def apply_full_copy(root: Path, check: bool) -> bool:
    changed = False
    for src_name, dst_rel in FULL_COPY:
        src = OVERRIDES / src_name
        dst = root / dst_rel
        if not src.exists():
            print(f"ОШИБКА: нет файла-рецепта {src}")
            sys.exit(1)

        if not dst.exists() or dst.read_bytes() != src.read_bytes():
            changed = True
            print(f"{'[check] заменить' if check else 'заменяю'}: {dst_rel}")
            if not check:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(src, dst)
    return changed


def apply_line_patches(root: Path, check: bool) -> bool:
    changed = False
    for patch in LINE_PATCHES:
        path = root / patch["file"]
        if not path.exists():
            print(f"ОШИБКА: файл для патча не найден: {patch['file']}")
            sys.exit(1)

        text = path.read_text(encoding="utf-8")

        if patch["test"] in text:
            continue

        if patch["prod"] not in text:
            print(
                f"ОШИБКА: в {patch['file']} не найдена ни боевая, ни тестовая "
                f"версия ожидаемой строки:\n  {patch['prod']!r}\n"
                f"Файл изменился в разработке — обнови рецепт в {Path(__file__).name} "
                f"под новую форму этой строки."
            )
            sys.exit(1)

        changed = True
        print(f"{'[check] патчить' if check else 'патчу'}: {patch['file']}")
        if not check:
            path.write_text(text.replace(patch["prod"], patch["test"], 1), encoding="utf-8")

    return changed


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-root",
        required=True,
        type=Path,
        help="путь к клону основного проекта (там, где src/ и front/)",
    )
    parser.add_argument("--check", action="store_true", help="показать изменения, не применяя")
    args = parser.parse_args()

    root = args.project_root.resolve()
    if not (root / "src" / "project").exists():
        print(f"ОШИБКА: по пути {root} не похоже на клон проекта (нет src/project)")
        sys.exit(1)

    changed = apply_full_copy(root, args.check)
    changed = apply_line_patches(root, args.check) or changed

    print()
    if not changed:
        print("Всё уже соответствует тестовому окружению, менять нечего.")
    elif args.check:
        print("Запусти без --check, чтобы применить.")
    else:
        print("Готово. Дальше — run-all.")


if __name__ == "__main__":
    main()