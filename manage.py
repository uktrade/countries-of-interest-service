#!/usr/bin/env python
import os
import subprocess
import sys

env = {**os.environ, 'FLASK_APP': "app.common.application:get_or_create()"}

cmd = ['flask'] + sys.argv[1:]
subprocess.run(cmd, env=env)
