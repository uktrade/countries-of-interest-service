from app.commands.algorithm import cmd_group as algorithm_cmd_group
from app.commands.csv import cmd_group as csv_cmd_group
from app.commands.database import cmd_group as database_cmd_group
from app.commands.dev import cmd_group as dev_cmd_group


def get_command_groups():
    return [algorithm_cmd_group, csv_cmd_group, database_cmd_group, dev_cmd_group]
