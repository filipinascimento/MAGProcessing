from pathlib import Path

# Set the PATH to MAG Folder
MAGPath = Path("/gpfs/sciencegenome/MAG/mag-2021-08-02/")
dataPath = Path("..")/"Data"
processedPath = dataPath/"Processed"
temporaryPath = dataPath/"Temporary"
sortedPath = MAGPath/"Sorted"

processedPath.mkdir(exist_ok=True)
temporaryPath.mkdir(exist_ok=True)

headersScriptPath = MAGPath/"samples"/"pyspark"/"MagClass.py"
magColumnsPath = dataPath/"MAGColumns.json"
