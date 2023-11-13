#!/usr/bin/env python
import sys
import shutil
from typing import Optional, List, Tuple, Dict

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "ELECT": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEADER": "#d0b343",
    "REQUESTVOTE": "#70c43f",
    "GETLOG": "#4878bc",
    "COPYLOG": "#398280",
    "COMMITLOG": "#98719f",
    "HEARTBEATS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
):
    f = open('printlog.log','w')
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False

    x = []
    for line in input_:
        time, *msg = line.strip().split(" ")
        time = int(time)
        x.append((msg,time))
    x.sort(key=lambda x:x[1])
    for i in x:
    
        topic = i[0][0]
            # To ignore some topics
        if topic not in topics:
                continue

        msg = " ".join(i[0][1:])
        j = int(msg[1])

        if colorize and topic in TOPICS:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            # Single column printing. Always the case for debug stmts in tests
     
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
       
        cols = ["" for _ in range(n_columns)]
        msg = "" + msg
        cols[j] = msg
        col_width = int(width / n_columns)
        cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
        print(cols,file=f)
       


if __name__ == "__main__":
    typer.run(main)