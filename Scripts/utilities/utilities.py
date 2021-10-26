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

    Args:
        magName (str): The name of table in the MAG dataset.
        sortColumnsNames (list): The names of the columns to sort by.

    Returns:
        str: The path to the sorted file.
    """
    return cfg.sortedPath/(magName+"_sorted_by%s.txt"%("-".join(sortColumnsNames)))

def getMAGPaths(magName):
    """
    Returns the filenames for a MAG dataset.

    Args:
        magName (str): The name of table in the MAG dataset.

    Returns:
        list: The paths to the files.
    """
    return list(cfg.MAGPath.glob(magColumnsData[magName]["Filename"]))

def getMAGMergedPath(magName):
    """
    Returns the merged for a MAG dataset.

    Args:
        magName (str): The name of table in the MAG dataset.

    Returns:
        str: The path to the merged file.
    """
    return cfg.sortedPath/(magName+"_merged.txt")

def getMAGCompressedPath(magName,sortColumnsNames=[]):
    """
    Returns the compressed filename for a MAG dataset.

    Args:
        magName (str): The name of table in the MAG dataset.
        sortColumnsNames (list): The names of the columns to sort by.

    Returns:
        str: The path to the compressed file.

    """
    if(sortColumnsNames):
        return cfg.processedPath/(magName+"_sorted_by%s.dbgz"%("-".join(sortColumnsNames)))
    else:
        return cfg.processedPath/(magName+".dbgz")


def getMAGIndexPath(magName,indexKey,sortColumnsNames=[]):
    """
    Returns the filename for the index of a MAG dataset.

    Args:
        magName (str): The name of table in the MAG dataset.
        indexKey (str): The name of the index.
        sortColumnsNames (list): The names of the columns to sort by.

    Returns:
        str: The path to the index file.

    """
    if(sortColumnsNames):
        return cfg.processedPath/(magName+"_sorted_by%s_indexedBy%s.idbgz"%("-".join(sortColumnsNames),indexKey))
    else:
        return cfg.processedPath/(magName+"_indexedBy%s.idbgz"%(indexKey))

def sortMAGFile(magName,sortColumnsNames,force=False,useSamstools=False):
    """
    Sorts the MAG files by sortColumnsNames

    Args:
        magName (str): The name of table in the MAG dataset.
        sortColumnsNames (list): The names of the columns to sort by.
        force (bool): If True, will overwrite the sorted file.
        useSamstools (bool): If True, will use samstools to sort the file.

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
    """
    Compresses the MAG files by sortColumnsNames

    Args:
        magName (str): The name of table in the MAG dataset.
        sortColumnsNames (list): The names of the columns to sort by.
        estimatedCount (int): The estimated number of lines in the file.

    """
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
                entries = line.decode("utf8").rstrip("\r\n").split("\t")
                encodedEntries = [entry[0] for entry in outfd.index2Type]
                for columnIndex,entry in enumerate(entries):
                    if(entry):
                        encodedEntries[columnIndex] = (SchemaParser[columnIndex][1](entry))
                outfd.writeFromArray(encodedEntries)
            pbar.refresh()
            pbar.close()



# def aggregateMAGFile(magName,newName,sortColumnsNames,aggrecatedColumns=[]):
#     """
#     Aggregates the MAG compressed files sorted by sortColumnsNames using the
#     first sorted  according
#     to the aggregation instructions in aggrecatedColumns.

#     Args:
#         magName (str): The name of table in the MAG dataset.
#         sortColumnsNames (list): The names of the columns to sort by.
#         aggrecatedColumns (list): The aggregation entries contains
#         the aggregation column names, keys and the aggregation functions.
#         Use (name,key,"first") to get the first value appearing for each
#         entry, (name,key,"list") to get a list of all values for each entry.

#     """
#     if(not sortColumnsNames):
#         raise Exception("sortColumnsNames must be specified")
#     if(newName in magColumnsData):
#         raise Exception("newName %s already exists"%(newName))
#     inputFilePath = getMAGCompressedPath(magName,sortColumnsNames)
#     outputFilePath = getMAGCompressedPath(newName)
#     SchemaParser = magColumnsData[magName]["SchemaParser"]
#     SchemaParserDictionary = dict(SchemaParser)
#     newSchemaParser = []
#     newSchemaParser.append((sortColumnsNames[0],SchemaParserDictionary[sortColumnsNames[0]]))
#     for columnName,key,aggregationFunction in aggrecatedColumns:
#         newSchemaParser.append((columnName,SchemaParserDictionary[columnName].upper()))
#     with dbgz.DBGZReader(inputFilePath) as infd:
#         with dbgz.DBGZWriter(outputFilePath,schema) as outfd:
#             for entry in tqdm(infd.entries,total=infd.entriesCount,desc="Authors Dataset"):
#                 paperID = entry["PaperId"]
#                 authorID = entry["AuthorId"]
#                 if(paperID in paper2Index):
#                     paperIndex = paper2Index[paperID]
#                     fdOut.write("%d\t%d\n"%(paperIndex,authorID))

