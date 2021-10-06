from tqdm.auto import tqdm
import os
from pathlib import Path
import struct
import os
import numpy as np
import subprocess
import gzip
import config as cfg
import utilities as ut

# Set the PATH to MAG Folder
MAGPath = cfg.MAGPath
dataPath = cfg.dataPath
processedPath = cfg.processedPath
temporaryPath = cfg.temporaryPath


print("Processing Journals...")
ut.compressMAGFile("Journals",["JournalId"])

print("Processing PaperAuthorAffiliations...")
ut.compressMAGFile("PaperAuthorAffiliations",["PaperId","AuthorId","AuthorSequenceNumber"])

print("Processing PaperAuthorAffiliations By Author...")
ut.compressMAGFile("PaperAuthorAffiliations",["AuthorId","AffiliationId"])

print("Processing Papers By JournalID...")
ut.compressMAGFile("Papers",["JournalId","Year"])


print("Processing PaperFieldsOfStudy...")
ut.compressMAGFile("PaperFieldsOfStudy",["PaperId","FieldOfStudyId"])

print("Processing PaperMeSH...")
ut.compressMAGFile("PaperMeSH",["PaperId","DescriptorUI","QualifierUI"])

print("Processing PaperReferences...")
ut.compressMAGFile("PaperReferences",["PaperId"])

print("Processing PaperReferences By Reference...")
ut.compressMAGFile("PaperReferences",["PaperReferenceId"])

print("Processing Papers...")
ut.compressMAGFile("Papers",["PaperId"])

print("Processing Abstracts...")
ut.compressMAGFile("PaperAbstractsInvertedIndex",["PaperId"])

print("Processing Authors...")
ut.compressMAGFile("Authors",["AuthorId"],estimatedCount=238938563)

# Create 
# Create merge and aggregate (some columns are aggregated)
# Filter function, create a new dbgz with only the requested columns


import config as cfg
import utilities as ut
import dbgz
from tqdm.auto import tqdm
compressedFilename = ut.getMAGCompressedPath("PaperReferences",["PaperReferenceId"])

with dbgz.DBGZReader(compressedFilename) as dataset:
    for entry in tqdm(dataset.entriesAsList,total=dataset.entriesCount):
        pass



def testReadSpeedMAGFile(magName,sortColumnsNames=[], estimatedCount = 0):
    if(sortColumnsNames):
        inputFilePath = ut.getMAGSortedPath(magName,sortColumnsNames)
    else:
        inputFilePath = ut.getMAGPaths(magName)[0]
    
    with open(inputFilePath,"rb") as infd:
        for line in tqdm(infd,total=estimatedCount):
            pass


testReadSpeedMAGFile("PaperReferences",["PaperReferenceId"])

PaperMeSH

# Citations  (edge list) reindexed
# AuthorID PaperID (reindex)
# PaperIndex PaperID Title Year VenueID MainMesh MeshHeading



