import sys
import yaml
import pandas as pd
import os
import logging
import time
import gc
import sqlalchemy as sa
import psycopg2
import mysql.connector
import traceback
import xlsxwriter
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
from tenacity import retry, stop_after_attempt, wait_exponential
from openpyxl import load_workbook

# Логирование
def get_logger():
    logger = logging.getLogger("ETL")
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/etl.log")
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

# Валидация миграции
def validate_migration(config, logger):
    source_type = config.get('source_type', 'excel')
    target_type = config.get('target_type', 'excel')
    
    try:
        if source_type == 'excel':
            src_df = pd.read_excel(config['source_file'], engine='openpyxl')
        else:
            engine = create_db_engine(source_type, config['db_params'])
            src_df = pd.read_sql(f"SELECT * FROM {config['db_params']['table']}", engine)

        if target_type == 'excel':
            tgt_df = pd.read_excel(config['target_file'], engine='openpyxl')
        else:
            engine = create_db_engine(target_type, config['db_params_target'])
            with engine.connect() as conn:
                table_exists = engine.dialect.has_table(conn, config['db_params_target']['table'])
            if not table_exists:
                logger.warning(f"⚠️ Target table '{config['db_params_target']['table']}' does not exist.")
                return False
            tgt_df = pd.read_sql(f"SELECT * FROM {config['db_params_target']['table']}", engine)

        src_count = len(src_df)
        tgt_count = len(tgt_df)
        logger.info(f"📊 Validation: Source rows = {src_count}, Target rows = {tgt_count}")
        
        if src_count != tgt_count:
            logger.warning("⚠️ Row count mismatch.")
            return False
        
        src_df = src_df.sort_index(axis=1)
        tgt_df = tgt_df.sort_index(axis=1)
        if not src_df.equals(tgt_df):
            logger.warning("⚠️ Data integrity mismatch.")
            return False
        
        logger.info("✅ Validation passed: row count and data integrity.")
        return True
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return False

# Создание SQLAlchemy движка
def create_db_engine(db_type, db_params):
    try:
        if db_type == 'postgresql':
            connection_string = (
                f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@"
                f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
            )
        elif db_type == 'mysql':
            connection_string = (
                f"mysql+mysqlconnector://{db_params['user']}:{db_params['password']}@"
                f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        return sa.create_engine(connection_string)
    except Exception as e:
        raise Exception(f"Failed to create database engine: {e}")

# Класс миграции
class ETLJob:
    def __init__(self, config):
        self.config = config
        self.source_type = config.get('source_type', 'excel')
        self.target_type = config.get('target_type', 'excel')
        self.source_file = config.get('source_file')
        self.target_file = config.get('target_file')
        self.db_params = config.get('db_params', {})
        self.db_params_target = config.get('db_params_target', {})
        self.chunk_size = config['chunk_size']
        self.commit_interval = config['commit_interval']
        self.logger = get_logger()
        self.total_rows = 0
        self.total_rows_expected = 0  # Определим позже
        self.progress_callback = config.get('progress_callback', None)
        self.duration = 0

    def stream_data(self):
        if self.source_type == 'excel':
            # Используем openpyxl для чтения Excel построчно
            workbook = load_workbook(self.source_file, read_only=True)
            sheet = workbook.active

            # Получаем заголовки
            headers = [cell.value for cell in next(sheet.rows)]
            if not headers:
                raise ValueError("Excel file is empty or has no headers")

            # Подсчитываем общее количество строк для прогресса
            self.total_rows_expected = sheet.max_row - 1  # Вычитаем строку заголовков
            self.logger.info(f"Total rows expected: {self.total_rows_expected}")

            # Собираем данные в чанки
            chunk_data = []
            for row in sheet.rows:
                # Пропускаем строку заголовков
                if row[0].row == 1:
                    continue
                row_data = [cell.value for cell in row]
                chunk_data.append(row_data)

                if len(chunk_data) >= self.chunk_size:
                    # Создаем DataFrame для текущего чанка
                    chunk = pd.DataFrame(chunk_data, columns=headers)
                    yield chunk
                    chunk_data = []  # Очищаем список для следующего чанка
                    gc.collect()

            # Обрабатываем оставшиеся строки
            if chunk_data:
                chunk = pd.DataFrame(chunk_data, columns=headers)
                yield chunk
                gc.collect()

            workbook.close()
        else:
            # Читаем из базы данных
            engine = create_db_engine(self.source_type, self.db_params)
            # Определяем общее количество строк
            with engine.connect() as conn:
                result = conn.execute(f"SELECT COUNT(*) FROM {self.db_params['table']}")
                self.total_rows_expected = result.scalar()
            self.logger.info(f"Total rows expected: {self.total_rows_expected}")

            query = f"SELECT * FROM {self.db_params['table']}"
            for chunk in pd.read_sql(query, engine, chunksize=self.chunk_size):
                yield chunk
                del chunk
                gc.collect()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _migrate_chunk_to_db(self, chunk):
        try:
            engine = create_db_engine(self.target_type, self.db_params_target)
            chunk.to_sql(
                self.db_params_target['table'],
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            self.logger.info(f"📝 Migrated {len(chunk)} rows to {self.target_type}")
        except Exception as e:
            self.logger.error(f"Ошибка миграции чанка в базу данных: {e}\n{traceback.format_exc()}")
            raise

    def run(self):
        self.logger.info(f"🚀 Starting ETL job: {self.source_type} -> {self.target_type}")
        start = time.time()
        chunk_num = 0

        try:
            if self.target_type == 'excel':
                # Потоковая запись в Excel с помощью xlsxwriter
                workbook = xlsxwriter.Workbook(self.target_file)
                worksheet = workbook.add_worksheet()
                first_chunk = True

                for chunk in self.stream_data():
                    chunk_num += 1
                    self.logger.info(f"Processing chunk {chunk_num}")

                    if first_chunk:
                        # Пишем заголовки
                        for col_num, col_name in enumerate(chunk.columns):
                            worksheet.write(0, col_num, col_name)
                        row = 1
                        first_chunk = False
                    else:
                        row = worksheet.dim_rowmax + 1 if worksheet.dim_rowmax is not None else 1

                    # Пишем данные чанка
                    for r, row_data in enumerate(chunk.itertuples(index=False), start=row):
                        for c, value in enumerate(row_data):
                            worksheet.write(r, c, value)

                    self.total_rows += len(chunk)
                    if self.progress_callback and self.total_rows_expected > 0:
                        progress = (self.total_rows / self.total_rows_expected) * 100
                        self.logger.info(f"📈 Migration progress: {progress:.2f}%")
                        self.progress_callback(progress)

                    self.logger.info(f"Chunk {chunk_num} processed")
                    del chunk
                    gc.collect()

                workbook.close()
                self.logger.info(f"📝 Migrated {self.total_rows} rows to {self.target_file}")
            else:
                # Для баз данных
                engine = create_db_engine(self.target_type, self.db_params_target)
                with engine.connect() as conn:
                    conn.execute(f"DROP TABLE IF EXISTS {self.db_params_target['table']}")
                self.logger.info(f"🧹 Target table '{self.db_params_target['table']}' cleared.")

                for chunk in self.stream_data():
                    chunk_num += 1
                    self.logger.info(f"Processing chunk {chunk_num}")
                    self._migrate_chunk_to_db(chunk)
                    self.total_rows += len(chunk)
                    if self.progress_callback and self.total_rows_expected > 0:
                        progress = (self.total_rows / self.total_rows_expected) * 100
                        self.logger.info(f"📈 Migration progress: {progress:.2f}%")
                        self.progress_callback(progress)
                    self.logger.info(f"Chunk {chunk_num} processed")
                    del chunk
                    gc.collect()

            self.duration = round(time.time() - start, 2)
            self.logger.info(f"✅ ETL finished in {self.duration}s, {self.total_rows} rows migrated.")
            
            validate_migration(self.config, self.logger)

        except Exception as e:
            self.logger.error(f"❌ ETL failed: {e}\n{traceback.format_exc()}")
            raise

# Загрузка конфигурации
def load_config(path='config.yaml'):
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Файл конфигурации {path} не найден.")
        sys.exit(1)
    
    for key in config:
        if isinstance(config[key], str):
            config[key] = config[key].format(**os.environ)
    return config

# Основная функция миграции
def run_etl(config):
    job = ETLJob(config)
    job.run()
    return {"status": "success", "rows_migrated": job.total_rows, "duration": job.duration}

# Диалог для ввода параметров базы данных
class DatabaseDialog(tk.Toplevel):
    def __init__(self, parent, title):
        super().__init__(parent)
        self.title(title)
        self.result = None
        self.geometry("400x400")

        tk.Label(self, text="Host:").pack()
        self.host_entry = tk.Entry(self)
        self.host_entry.insert(0, "localhost")
        self.host_entry.pack()

        tk.Label(self, text="Port:").pack()
        self.port_entry = tk.Entry(self)
        self.port_entry.insert(0, "5432" if "PostgreSQL" in title else "3306")
        self.port_entry.pack()

        tk.Label(self, text="Database:").pack()
        self.db_entry = tk.Entry(self)
        self.db_entry.pack()

        tk.Label(self, text="User:").pack()
        self.user_entry = tk.Entry(self)
        self.user_entry.pack()

        tk.Label(self, text="Password:").pack()
        self.password_entry = tk.Entry(self, show="*")
        self.password_entry.pack()

        tk.Label(self, text="Table:").pack()
        self.table_entry = tk.Entry(self)
        self.table_entry.pack()

        tk.Button(self, text="OK", command=self.on_ok).pack(pady=10)
        tk.Button(self, text="Cancel", command=self.on_cancel).pack()

    def on_ok(self):
        self.result = {
            "host": self.host_entry.get(),
            "port": self.port_entry.get(),
            "database": self.db_entry.get(),
            "user": self.user_entry.get(),
            "password": self.password_entry.get(),
            "table": self.table_entry.get()
        }
        self.destroy()

    def on_cancel(self):
        self.result = None
        self.destroy()

# Графический интерфейс
class ETLApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL Migration Tool")
        self.logger = get_logger()
        self.config = load_config()

        tk.Label(root, text="ETL Migration Tool", font=("Arial", 16)).pack(pady=10)

        tk.Label(root, text="Тип источника:").pack()
        self.source_type = tk.StringVar(value="excel")
        source_types = ["excel", "postgresql", "mysql"]
        self.source_type_menu = ttk.Combobox(root, textvariable=self.source_type, values=source_types, state="readonly")
        self.source_type_menu.pack()
        self.source_type_menu.bind("<<ComboboxSelected>>", self.on_source_type_change)

        tk.Label(root, text="Исходный файл или база данных:").pack()
        self.source_file_entry = tk.Entry(root, width=50)
        self.source_file_entry.pack()
        self.source_file_button = tk.Button(root, text="Выбрать исходный файл", command=self.select_source_file)
        self.source_file_button.pack(pady=5)

        tk.Label(root, text="Тип цели:").pack()
        self.target_type = tk.StringVar(value="excel")
        target_types = ["excel", "postgresql", "mysql"]
        self.target_type_menu = ttk.Combobox(root, textvariable=self.target_type, values=target_types, state="readonly")
        self.target_type_menu.pack()
        self.target_type_menu.bind("<<ComboboxSelected>>", self.on_target_type_change)

        tk.Label(root, text="Целевой файл или база данных:").pack()
        self.target_file_entry = tk.Entry(root, width=50)
        self.target_file_entry.pack()
        self.target_file_button = tk.Button(root, text="Выбрать целевой файл", command=self.select_target_file)
        self.target_file_button.pack(pady=5)

        tk.Label(root, text="Chunk Size:").pack()
        self.chunk_size_entry = tk.Entry(root)
        self.chunk_size_entry.insert(0, str(self.config.get("chunk_size", 10000)))
        self.chunk_size_entry.pack()

        tk.Label(root, text="Commit Interval:").pack()
        self.commit_interval_entry = tk.Entry(root)
        self.commit_interval_entry.insert(0, str(self.config.get("commit_interval", 20000)))
        self.commit_interval_entry.pack()

        tk.Button(root, text="Запустить миграцию", command=self.run_migration).pack(pady=5)

        self.progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(pady=10)

        self.log_area = scrolledtext.ScrolledText(root, width=80, height=20)
        self.log_area.pack(pady=10)

    def log(self, message):
        def update_log():
            self.log_area.insert(tk.END, message + "\n")
            self.log_area.yview(tk.END)
        self.root.after(0, update_log)
        self.logger.info(message)

    def on_source_type_change(self, event):
        source_type = self.source_type.get()
        if source_type == 'excel':
            self.source_file_button.config(text="Выбрать исходный файл", command=self.select_source_file)
        else:
            self.source_file_button.config(text="Выбрать базу данных", command=self.select_source_db)

    def on_target_type_change(self, event):
        target_type = self.target_type.get()
        if target_type == 'excel':
            self.target_file_button.config(text="Выбрать целевой файл", command=self.select_target_file)
        else:
            self.target_file_button.config(text="Выбрать базу данных", command=self.select_target_db)

    def select_source_file(self):
        file_path = filedialog.askopenfilename(
            title="Выберите исходный файл",
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.source_file_entry.delete(0, tk.END)
            self.source_file_entry.insert(0, file_path)
            self.log(f"Выбран исходный файл: {file_path}")

    def select_target_file(self):
        file_path = filedialog.asksaveasfilename(
            title="Выберите целевой файл",
            filetypes=[("Excel files", "*.xlsx *.xls")],
            defaultextension=".xlsx"
        )
        if file_path:
            self.target_file_entry.delete(0, tk.END)
            self.target_file_entry.insert(0, file_path)
            self.log(f"Выбран целевой файл: {file_path}")

    def select_source_db(self):
        dialog = DatabaseDialog(self.root, f"Параметры {self.source_type.get().upper()} источника")
        self.root.wait_window(dialog)
        if dialog.result:
            self.source_file_entry.delete(0, tk.END)
            self.source_file_entry.insert(0, f"{self.source_type.get()}://{dialog.result['host']}:{dialog.result['port']}/{dialog.result['database']}")
            self.db_params = dialog.result
            self.log(f"Выбрана база данных источника: {self.source_file_entry.get()}")

    def select_target_db(self):
        dialog = DatabaseDialog(self.root, f"Параметры {self.target_type.get().upper()} цели")
        self.root.wait_window(dialog)
        if dialog.result:
            self.target_file_entry.delete(0, tk.END)
            self.target_file_entry.insert(0, f"{self.source_type.get()}://{dialog.result['host']}:{dialog.result['port']}/{dialog.result['database']}")
            self.db_params_target = dialog.result
            self.log(f"Выбрана база данных цели: {self.target_file_entry.get()}")

    def run_migration(self):
        threading.Thread(target=self._run_migration, daemon=True).start()

    def _run_migration(self):
        source_type = self.source_type.get()
        target_type = self.target_type.get()
        source_file = self.source_file_entry.get()
        target_file = self.target_file_entry.get()
        if not source_file or not target_file:
            messagebox.showwarning("Предупреждение", "Выберите исходный и целевой файлы или базы данных.")
            return
        self.log("Запуск миграции...")
        self.progress["value"] = 0
        try:
            self.config['source_type'] = source_type
            self.config['target_type'] = target_type
            self.config['source_file'] = source_file
            self.config['target_file'] = target_file
            self.config['db_params'] = getattr(self, 'db_params', {})
            self.config['db_params_target'] = getattr(self, 'db_params_target', {})
            self.config['chunk_size'] = int(self.chunk_size_entry.get())
            self.config['commit_interval'] = int(self.commit_interval_entry.get())
            def update_progress(value):
                def update():
                    self.progress["value"] = value
                self.root.after(0, update)

            self.config['progress_callback'] = update_progress

            result = run_etl(self.config)

            self.log(f"Миграция завершена: {result}")
            self.progress["value"] = 100

        except Exception as e:
            self.log(f"❌ Миграция не удалась: {str(e)}\n{traceback.format_exc()}")
            messagebox.showerror("Ошибка", f"Миграция не удалась: {str(e)}")

# Точка входа
if __name__ == "__main__":
    root = tk.Tk()
    app = ETLApp(root)
    root.mainloop()