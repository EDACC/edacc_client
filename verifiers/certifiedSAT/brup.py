#!/usr/bin/env python2

import subprocess, sys

proc = subprocess.Popen(["./brup2drup | ./drup-check \"" + sys.argv[1] + "\""], shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
proc.stdin.write(sys.stdin.read())
proc.stdin.close()
proc.wait()
sys.stdout.write(proc.stdout.read())
