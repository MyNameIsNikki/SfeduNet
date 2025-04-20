import sys
import yaml
import pandas as pd
import joblib
import os
import logging
import requests
import random
import time
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
import psycopg2
from prometheus_client import Counter, Histogram
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import mlflow
import mlflow.xgboost
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading

# Метрики Prometheus
migrations_total = Counter('etl_migrations_total', 'Total number of ETL migrations')
migration_duration = Histogram('etl_migration_duration_seconds', 'Duration of ETL migrations')
rows_migrated = Counter('etl_rows_migrated_total', 'Total rows migrated')

# Логирование
class SlackHandler(logging.Handler):
    def __init__(self, webhook_url):
        super().__init__()
        self.webhook_url = webhook_url

    def emit(self, record):
        if record.levelno >= logging.ERROR:
            msg = self.format(record)
            requests.post(self.webhook_url, json={"text": msg})

def get_logger():
    logger = logging.getLogger("ETL")
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/etl.log")
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if slack_webhook:
        slack_handler = SlackHandler(slack_webhook)
        slack_handler.setFormatter(formatter)
        logger.addHandler(slack_handler)
    return logger

# Извлечение признаков из данных
def extract_features(config, data=None):
    if data is not None:
        # Анализ данных из pandas DataFrame
        num_cols = len(data.columns)
        num_rows = len(data)
        text_cols = sum(data.dtypes.apply(lambda x: x == "object"))
        numeric_cols = sum(data.dtypes.apply(lambda x: x in [int, float]))
        avg_row_size = data.memory_usage(deep=True).sum() / num_rows if num_rows > 0 else 0
        total_amount = data['Сумма ВТ'].sum() if 'Сумма ВТ' in data.columns else 0
        total_quantity = data['Количество'].sum() if 'Количество' in data.columns else 0
    else:
        # Анализ из базы данных
        engine = create_engine(config['source_db'])
        with engine.connect() as conn:
            num_cols = conn.execute(text(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = '{config['table']}'
            """)).scalar()
            num_rows = conn.execute(text(f"SELECT COUNT(*) FROM {config['table']}")).scalar()
            col_types = conn.execute(text(f"""
                SELECT data_type, COUNT(*) as count
                FROM information_schema.columns
                WHERE table_name = '{config['table']}'
                GROUP BY data_type
            """)).fetchall()
            type_dict = {row['data_type']: row['count'] for row in col_types}
            avg_row_size = conn.execute(text(f"""
                SELECT AVG(pg_column_size(t.*)) as avg_size
                FROM {config['table']} t
                LIMIT 1000
            """)).scalar() or 0
            text_cols = type_dict.get("character varying", 0) + type_dict.get("text", 0)
            numeric_cols = type_dict.get("integer", 0) + type_dict.get("numeric", 0)
            total_amount = conn.execute(text(f"SELECT SUM(\"Сумма ВТ\") FROM {config['table']}")).scalar() or 0
            total_quantity = conn.execute(text(f"SELECT SUM(\"Количество\") FROM {config['table']}")).scalar() or 0

    return {
        "num_columns": num_cols,
        "num_rows": num_rows,
        "source_engine": config['source_db'].split(":")[0] if 'source_db' in config else "unknown",
        "target_engine": config['target_db'].split(":")[0] if 'target_db' in config else "unknown",
        "text_columns": text_cols,
        "numeric_columns": numeric_cols,
        "avg_row_size": avg_row_size,
        "total_amount": total_amount,
        "total_quantity": total_quantity
    }

# Предобработка данных
def preprocess_data(df):
    # Удаление дубликатов
    df = df.drop_duplicates()
    # Кодирование категориальных столбцов
    le = LabelEncoder()
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = le.fit_transform(df[col].astype(str))
    return df

# Валидация миграции
def validate_migration(src_engine, tgt_engine, table, logger):
    with src_engine.connect() as src, tgt_engine.connect() as tgt:
        src_count = src.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        tgt_count = tgt.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        logger.info(f"📊 Validation: Source = {src_count}, Target = {tgt_count}")
        if src_count != tgt_count:
            logger.warning("⚠️ Row count mismatch.")
            return False
        src_sample = pd.read_sql(text(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 100"), src)
        tgt_sample = pd.read_sql(text(f"SELECT * FROM {table} ORDER BY RANDOM() LIMIT 100"), tgt)
        if not src_sample.equals(tgt_sample):
            logger.warning("⚠️ Data integrity mismatch in sampled rows.")
            return False
        logger.info("✅ Validation passed: row count and data integrity.")
        return True

# Класс миграции
class ETLJob:
    def __init__(self, config, data=None):
        self.config = config
        self.data = preprocess_data(data) if data is not None else None
        self.src_engine = create_engine(config['source_db']) if self.data is None else None
        self.tgt_engine = create_engine(config['target_db'])
        self.table = config['table']
        self.chunk_size = config['chunk_size']
        self.commit_interval = config['commit_interval']
        self.parallel_chunks = config.get('parallel_chunks', 1)
        self.logger = get_logger()
        self.total_rows = 0
        self.duration = 0

    def stream_data(self):
        if self.data is not None:
            for i in range(0, len(self.data), self.chunk_size):
                yield self.data[i:i + self.chunk_size]
        else:
            with self.src_engine.connect().execution_options(stream_results=True) as conn:
                result = conn.execution_options(yield_per=self.chunk_size).execute(text(f"SELECT * FROM {self.table}"))
                while True:
                    chunk = result.fetchmany(self.chunk_size)
                    if not chunk:
                        break
                    df = pd.DataFrame(chunk, columns=result.keys())
                    yield df

    def _copy_to_postgres(self, df, conn):
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cursor = conn.cursor()
        cursor.copy_from(output, self.table, sep='\t', null='')
        conn.commit()

    def _migrate_chunk(self, chunk, tgt_conn):
        try:
            with tgt_conn.connect() as conn:
                self._copy_to_postgres(chunk, conn.raw_connection())
                rows_migrated.inc(len(chunk))
        except Exception as e:
            self.logger.error(f"Ошибка миграции чанка: {e}")
            raise

    def run(self, parallel=False):
        self.logger.info(f"🚀 Starting ETL job for table '{self.table}'")
        migrations_total.inc()
        start = time.time()
        buffer = []

        try:
            with self.tgt_engine.connect() as conn:
                conn.execute(text(f"DELETE FROM {self.table}"))
                self.logger.info(f"🧹 Target table '{self.table}' cleared.")

            with self.tgt_engine.begin() as tgt_conn:
                chunks = self.stream_data()
                if parallel:
                    with ThreadPoolExecutor(max_workers=self.parallel_chunks) as executor:
                        for chunk in chunks:
                            buffer.append(chunk)
                            self.total_rows += len(chunk)
                            if self.total_rows % self.commit_interval < self.chunk_size:
                                executor.submit(self._migrate_chunk, pd.concat(buffer), tgt_conn)
                                buffer = []
                else:
                    for chunk in chunks:
                        buffer.append(chunk)
                        self.total_rows += len(chunk)
                        if self.total_rows % self.commit_interval < self.chunk_size:
                            self._copy_to_postgres(pd.concat(buffer), tgt_conn.raw_connection())
                            buffer = []

                if buffer:
                    self._copy_to_postgres(pd.concat(buffer), tgt_conn.raw_connection())

            self.duration = round(time.time() - start, 2)
            migration_duration.observe(self.duration)
            self.logger.info(f"✅ ETL finished in {self.duration}s, {self.total_rows} rows migrated.")
            
            with self.tgt_engine.connect() as conn:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{self.table}_id ON {self.table} (id)"))
                self.logger.info(f"📈 Index added on {self.table}.id")

            if self.data is None:
                validate_migration(self.src_engine, self.tgt_engine, self.table, self.logger)

        except Exception as e:
            self.logger.error(f"❌ ETL failed: {e}")
            raise

# Генетический алгоритм
def migrate_once(chunk_size, commit_interval, config, data=None):
    start = time.time()
    config['chunk_size'] = chunk_size
    config['commit_interval'] = commit_interval
    job = ETLJob(config, data)
    job.run()
    
    duration = time.time() - start
    data_loss = 0  # Для упрощения, в реальном коде нужно сравнивать данные
    total_amount = config.get('features', {}).get('total_amount', 0)
    fitness = (1.0 / (duration + 1)) * (1 - data_loss) * (1 + total_amount / 1e6)  # Учитываем объём транзакций
    
    feats = config.get('features', {})
    result = {
        "num_columns": feats.get("num_columns", 0),
        "num_rows": feats.get("num_rows", 0),
        "source_engine": feats.get("source_engine", ""),
        "target_engine": feats.get("target_engine", ""),
        "text_columns": feats.get("text_columns", 0),
        "numeric_columns": feats.get("numeric_columns", 0),
        "avg_row_size": feats.get("avg_row_size", 0),
        "total_amount": feats.get("total_amount", 0),
        "total_quantity": feats.get("total_quantity", 0),
        "best_batch": chunk_size,
        "best_commit": commit_interval,
        "duration": duration,
        "data_loss": data_loss,
    }
    df = pd.DataFrame([result])
    os.makedirs("ml_model", exist_ok=True)
    if os.path.exists("ml_model/dataset.csv"):
        existing_df = pd.read_csv("ml_model/dataset.csv")
        if set(existing_df.columns) == set(result.keys()):
            df.to_csv("ml_model/dataset.csv", mode='a', header=False, index=False)
        else:
            print("⚠️ Структура dataset.csv не совпадает, создаём новый файл.")
            df.to_csv("ml_model/dataset.csv", mode='w', header=True, index=False)
    else:
        df.to_csv("ml_model/dataset.csv", mode='w', header=True, index=False)
    
    return fitness

def genetic_algorithm(config, data=None, pop_size=6, generations=4, initial_params=None):
    if initial_params:
        population = [[initial_params[0], initial_params[1]]]
        population.extend([[random.choice([5000, 10000, 20000]), random.choice([10000, 20000, 50000])] for _ in range(pop_size-1)])
    else:
        population = [[random.choice([5000, 10000, 20000]), random.choice([10000, 20000, 50000])] for _ in range(pop_size)]
    
    for gen in range(generations):
        scored = [(migrate_once(p[0], p[1], config, data), p) for p in population]
        scored.sort(reverse=True)
        best = scored[0][1]
        print(f"Gen {gen+1}: best={best}")
        elites = [p for _, p in scored[:pop_size//2]]
        children = []
        while len(children) < pop_size - len(elites):
            p1, p2 = random.sample(elites, 2)
            child = [
                random.choice([p1[0], p2[0]]),
                int((p1[1] + p2[1]) / 2 + random.randint(-5000, 5000))
            ]
            children.append(child)
        population = elites + children
    return best

# Обучение ML-модели (XGBoost)
def train_model(data=None):
    try:
        mlflow.set_experiment("etl_migration_optimization")
    except Exception as e:
        print(f"⚠️ Не удалось подключиться к MLflow: {e}. Логирование экспериментов отключено.")

    try:
        df = pd.read_csv("ml_model/dataset.csv")
    except FileNotFoundError:
        print("❌ Файл dataset.csv не найден. Создайте его с помощью миграции.")
        return

    X = df.drop(columns=["best_batch", "best_commit", "duration", "data_loss"])
    y_batch = df["best_batch"]
    y_commit = df["best_commit"]

    with mlflow.start_run():
        model_batch = xgb.XGBRegressor(n_estimators=100, random_state=42)
        model_commit = xgb.XGBRegressor(n_estimators=100, random_state=42)

        X_train, X_test, yb_train, yb_test = train_test_split(X, y_batch, test_size=0.2, random_state=42)
        model_batch.fit(X_train, yb_train)
        batch_mae = mean_absolute_error(yb_test, model_batch.predict(X_test))
        print("Batch MAE:", batch_mae)

        X_train, X_test, yc_train, yc_test = train_test_split(X, y_commit, test_size=0.2, random_state=42)
        model_commit.fit(X_train, yc_train)
        commit_mae = mean_absolute_error(yc_test, model_commit.predict(X_test))
        print("Commit MAE:", commit_mae)

        mlflow.log_metric("batch_mae", batch_mae)
        mlflow.log_metric("commit_mae", commit_mae)
        mlflow.xgboost.log_model(model_batch, "model_batch")
        mlflow.xgboost.log_model(model_commit, "model_commit")

        os.makedirs("ml_model", exist_ok=True)
        joblib.dump(model_batch, "ml_model/model_batch.pkl")
        joblib.dump(model_commit, "ml_model/model_commit.pkl")

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
def run_etl(config, data=None, parallel=True):
    if config.get("optimize", False):
        feats = extract_features(config, data)
        config['features'] = feats
        feats_df = pd.DataFrame([feats])
        
        try:
            model_batch = joblib.load("ml_model/model_batch.pkl")
            model_commit = joblib.load("ml_model/model_commit.pkl")
        except FileNotFoundError:
            print("❌ Модели не найдены. Запустите 'python etl_project.py train' для их создания.")
            sys.exit(1)
        
        pred_batch = int(model_batch.predict(feats_df)[0])
        pred_commit = int(model_commit.predict(feats_df)[0])
        
        # Оптимизация с помощью ГА
        best_params = genetic_algorithm(config, data, initial_params=[pred_batch, pred_commit])
        config['chunk_size'] = best_params[0]
        config['commit_interval'] = best_params[1]
        print(f"🤖 Оптимизированные параметры: batch = {best_params[0]}, commit = {best_params[1]}")

    job = ETLJob(config, data)
    job.run(parallel=parallel)
    return {"status": "success", "rows_migrated": job.total_rows, "duration": job.duration}

# REST API
app = FastAPI()

class MigrationRequest(BaseModel):
    config_path: str = "config.yaml"
    parallel: bool = True

@app.post("/migrate")
async def migrate(request: MigrationRequest):
    try:
        config = load_config(request.config_path)
        result = run_etl(config, parallel=request.parallel)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")

@app.get("/health")
async def health():
    return {"status": "healthy"}

# Графический интерфейс
class ETLApp:
    def __init__(self, root):
        self.root = root
        self.root.title("ETL Migration Tool")
        self.logger = get_logger()
        self.config = load_config()
        self.data = None

        # Элементы интерфейса
        tk.Label(root, text="ETL Migration Tool", font=("Arial", 16)).pack(pady=10)

        # Загрузка данных
        tk.Button(root, text="Загрузить данные (Excel)", command=self.load_data).pack(pady=5)

        # Обучение модели
        tk.Button(root, text="Обучить модель", command=self.train_model).pack(pady=5)

        # Запуск миграции
        tk.Button(root, text="Запустить миграцию", command=self.run_migration).pack(pady=5)

        # Прогресс
        self.progress = ttk.Progressbar(root, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(pady=10)

        # Логи
        self.log_area = scrolledtext.ScrolledText(root, width=80, height=20)
        self.log_area.pack(pady=10)

    def log(self, message):
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.yview(tk.END)
        self.logger.info(message)

    def load_data(self):
        try:
            self.data = pd.read_excel("Book1.xlsx")
            self.log(f"Данные загружены: {len(self.data)} строк, {len(self.data.columns)} столбцов.")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить данные: {str(e)}")

    def train_model(self):
        threading.Thread(target=self._train_model, daemon=True).start()

    def _train_model(self):
        self.log("Начало обучения модели...")
        self.progress["value"] = 0
        train_model(self.data)
        self.log("Модель обучена.")
        self.progress["value"] = 100

    def run_migration(self):
        threading.Thread(target=self._run_migration, daemon=True).start()

    def _run_migration(self):
        if self.data is None:
            messagebox.showwarning("Предупреждение", "Сначала загрузите данные.")
            return
        self.log("Запуск миграции...")
        self.progress["value"] = 0
        try:
            result = run_etl(self.config, self.data, parallel=True)
            self.log(f"Миграция завершена: {result}")
            self.progress["value"] = 100
        except Exception as e:
            messagebox.showerror("Ошибка", f"Миграция не удалась: {str(e)}")

# Точка входа
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "gui"

    if mode == "train":
        train_model()
    elif mode == "migrate":
        config = load_config()
        run_etl(config)
    elif mode == "api":
        port = int(os.getenv("API_PORT", 8000))
        uvicorn.run(app, host="0.0.0.0", port=port)
    elif mode == "gui":
        root = tk.Tk()
        app = ETLApp(root)
        root.mainloop()
    else:
        print("Usage: python etl_project.py [train|migrate|api|gui]")