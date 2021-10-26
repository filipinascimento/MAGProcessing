from tqdm.auto import tqdm
import os
from pathlib import Path
import struct
import os
import numpy as np
import subprocess
import gzip
import json
import config as cfg
from pathlib import Path
# Set the PATH to MAG Folder

MAGPath = cfg.MAGPath
dataPath = cfg.dataPath

headersScriptPath = cfg.headersScriptPath

# This may change in future, please update accordingly
headerStreamData = ""
with open(headersScriptPath, "r") as fd:
    for line in fd:
        line = line.strip()
        if(headerStreamData):
            headerStreamData += line
            if(line.startswith("}")):
                break
        elif(line.startswith("streams")):
            headerStreamData+=line;

exec(headerStreamData)

with open(dataPath/"MAGColumns.json","wt") as fd:
    json.dump(streams,fd)

