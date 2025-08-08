import subprocess
import datetime
import os

print(f"🚀 ETL Triggered at {datetime.datetime.now()}")

try:
    subprocess.run(["python", "etl.py"], check=True)
    print("✅ ETL completed successfully")
except subprocess.CalledProcessError as e:
    print("❌ ETL failed:", e)

log_path = os.getenv("LOG_PATH", "logs/etl.log")
with open(log_path, "a") as log:
    log.write(f"{datetime.datetime.now()} - ETL run completed\n")