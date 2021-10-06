import config as cfg
import json
import subprocess
import dbgz
from tqdm.auto import tqdm

# def schemaFromMAGColumns(magColumns):
#     """
#     Returns a schema from a list of MAG columns.
#     """

with open(cfg.magColumnsPath) as fd:
    MAGColumnsRAW = json.load(fd)


typeToSchemaDictionary = {
    "long":("i",int),
    "float":("d",float),
    "int":("i",int),
    "uint":("u",int),
}

magColumnsData = {}
for key,(filename,columns) in MAGColumnsRAW.items():
    magColumnsData[key] = {
        "Filename":filename.replace("{*}","*"),
        "Columns":columns,
    }
    schema = []
    schemaParser = []
    column2Index = {}
    for columnIndex,columnData in enumerate(columns):
        columnName, columnType = columnData.strip("?").split(":")
        typeLetter,typeFunction = ("s",str)
        if(columnType in typeToSchemaDictionary):
            typeLetter,typeFunction = typeToSchemaDictionary[columnType]
        schema.append((columnName,typeLetter))
        schemaParser.append((columnName,typeFunction))
        column2Index[columnName] = columnIndex
    magColumnsData[key]["Schema"] = schema
    magColumnsData[key]["SchemaParser"] = schemaParser
    magColumnsData[key]["Column2Index"] = column2Index


def getMAGSortedPath(magName,sortColumnsNames):
    """
    Returns the sorted filename for a MAG dataset.
    """
    return cfg.sortedPath/(magName+"_sorted_by%s.txt"%("-".join(sortColumnsNames)))

def getMAGPaths(magName):
    """
    Returns the filenames for a MAG dataset.
    """
    return list(cfg.MAGPath.glob(magColumnsData[magName]["Filename"]))

def getMAGMergedPath(magName):
    """
    Returns the merged for a MAG dataset.
    """
    return cfg.sortedPath/(magName+"_merged.txt")

def getMAGCompressedPath(magName,sortColumnsNames=[]):
    """
    Returns the compressed filename for a MAG dataset.
    """
    if(sortColumnsNames):
        return cfg.processedPath/(magName+"_sorted_by%s.dbgz"%("-".join(sortColumnsNames)))
    else:
        return cfg.processedPath/(magName+".dbgz")


def getMAGIndexPath(magName,indexKey,sortColumnsNames=[]):
    """
    Returns the filename for the index of a MAG dataset.
    """
    if(sortColumnsNames):
        return cfg.processedPath/(magName+"_sorted_by%s.idbgz"%("-".join(sortColumnsNames)))
    else:
        return cfg.processedPath/(magName+".idbgz")

def sortMAGFile(magName,sortColumnsNames,force=False,useSamstools=False):
    """
    Sorts the MAG files by sortColumnsNames
    """
    filePaths = getMAGPaths(magName)
    sortedFilePath = getMAGSortedPath(magName,sortColumnsNames)
    #!sort --parallel=20 -T../../Temp/ -t$'\t' -nk1 -nk2 -nk4 ../../Data/ProcessedNew/JournalAuthorYearPaper.txt > ../../Data/ProcessedNew/JournalAuthorYearPaper_byJournal.txt
    sortArguments = [
        "sort",
        "--parallel=20",
        "-T",str(cfg.temporaryPath),
        "-t",
        "\t"
    ]
    if(useSamstools):
        sortArguments.append("--compress-program")
        sortArguments.append("bgzip")
    for columnName in sortColumnsNames:
        columnIndex = magColumnsData[magName]["Column2Index"][columnName]
        _,columnType = magColumnsData[magName]["Schema"][columnIndex]
        if(columnType=="i" or columnType=="d" or columnType=="u"):
            sortArguments.append("-nk%d"%(columnIndex+1))
        else:
            sortArguments.append("-k%d"%(columnIndex+1))

    inputFilePath = ""
    if(len(filePaths)==1):
        inputFilePath = filePaths[0]
    else:
        inputFilePath = getMAGMergedPath(magName)
        if(force or not inputFilePath.exists()):
            print("Merging %d files"%(len(filePaths)))
            with open(inputFilePath,"w") as fd:
                subprocess.run(["cat"]+[str(path) for path in filePaths],stdout=fd)
        else:
            print("Merged file exists, skipping...")
    sortArguments.append(str(inputFilePath))
    # print(sortArguments)
    print("Executing command:\n %s"%(" ".join(sortArguments)))
    if(force or not sortedFilePath.exists()):
        print("Sorting %s"%(magName))
        with open(sortedFilePath,"wb") as fd:
            subprocess.run(sortArguments,stdout=fd)
    else:
        print("Sorted file exists, skipping...")



# bgzDescriptor = {
#     "name" = "Authors_Paper_Affiliation_Year"

#     {
#         "magName":"JournalAuthorYearPaper",
#         "extraSortColumns": [],
#         "columns": [
#         ]
#         "processColumn":{

#         }
#     }
# ]

def compressMAGFile(magName,sortColumnsNames=[], estimatedCount = 0):
    if(sortColumnsNames):
        inputFilePath = getMAGSortedPath(magName,sortColumnsNames)
    else:
        inputFilePath = getMAGPaths(magName)[0]
    compressedFilePath = getMAGCompressedPath(magName,sortColumnsNames)
    schema = magColumnsData[magName]["Schema"]
    SchemaParser = magColumnsData[magName]["SchemaParser"]
    # New entries can be added as:
    
    with open(inputFilePath,"rb") as infd:
        with dbgz.DBGZWriter(compressedFilePath,schema) as outfd:
            pbar = tqdm(total=estimatedCount)
            for line in infd:
                pbar.update(1)
                entries = line.decode("utf8").strip("\n").split("\t")
                encodedEntries = [entry[0] for entry in outfd.index2Type]
                for columnIndex,entry in enumerate(entries):
                    if(entry):
                        encodedEntries[columnIndex] = (SchemaParser[columnIndex][1](entry))
                outfd.writeFromArray(encodedEntries)
            pbar.refresh()
            pbar.close()