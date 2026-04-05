import pandas
import watchdog
import sqlalchemy
import dotenv
import tenacity

print("All dependencies installed successfully.")
print("pandas:", pandas.__version__)
print("watchdog:", watchdog.__version__ if hasattr(watchdog, "__version__") else "installed")
print("sqlalchemy:", sqlalchemy.__version__)
print("dotenv: installed")
print("tenacity:", tenacity.__version__ if hasattr(tenacity, "__version__") else "installed")