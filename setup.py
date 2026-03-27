from subprocess import check_call
from sys import executable

def setup():
    check_call([executable, "-m", "pip", "install", "-r", "requirements.txt"])
    check_call([executable, "-m", "playwright", "install", "chromium"])