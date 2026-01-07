import logging

logging.basicConfig(
    filename="error.log",          # 🔹 error file
    level=logging.ERROR,           # 🔹 only errors
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"    # 🔹 date time format
)

def test():
    return 10 / 0   # error

try:
    test()
except Exception:
    logging.exception("Runtime error occurred")
