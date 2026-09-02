## Итоговый цикл запуска

1. Удалить старую копию (если обновляете на том же пути) или клонировать в новую директорию
2. Клонировать репозиторий:
   ```bash
   git clone <репозиторий>
   ```
3. Посмотреть, что изменится:
   ```bash
   python apply_test_overrides.py --project-root /opt/my_script/service --check
   ```
4. Применить:
   ```bash
   python apply_test_overrides.py --project-root /opt/my_script/service
   ```
5. Запустить сервис:
   ```bash
   run-all
   ```