#!/usr/bin/env python
import os
import time
from pathlib import Path
import shutil

STREAM_IN = '../stream-IN'
SLEEP = 3 #seconds


# Get the full path to this script
scriptPath = Path(os.path.realpath(__file__))

# calculate the path of the data dir
splitDataFolder = scriptPath.parent / 'data' / 'split'

# calculate the path of the destination dir
destinationFolder = scriptPath.parent / STREAM_IN

# we will process the files sorted on the file name
files = splitDataFolder.glob("*.tmp")

# files that already exist in the destination folder are skipped
existingFiles = [x.name for x in destinationFolder.glob("*.tmp")]
files = [x for x in files if x.name not in existingFiles]

try:
    # process files in sorted order
    for f in sorted(files):
        print("Copying %s -> %s" % (f.name, destinationFolder.name + "/" + f.name))
        shutil.copy(f, destinationFolder)
        time.sleep(SLEEP)

    print("\n DONE - all files copied")
except KeyboardInterrupt:
    print("Terminating ...")
