from asyncio import run
from setup import setup
from parser import WBParser

if __name__ == "__main__":
    setup()
    parser = WBParser()
    run(parser.search("пальто из натуральной шерсти"))
