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
ut.sortMAGFile("Journals",["JournalId"],useSamstools=True)


print("Processing PaperAuthorAffiliations...")
ut.sortMAGFile("PaperAuthorAffiliations",["PaperId","AuthorId","AuthorSequenceNumber"],useSamstools=True)

print("Processing PaperAuthorAffiliations By Author...")
ut.sortMAGFile("PaperAuthorAffiliations",["AuthorId","AffiliationId"],useSamstools=True)

print("Processing Papers By JournalID...")
ut.sortMAGFile("Papers",["JournalId","Year"],useSamstools=True)


print("Processing PaperFieldsOfStudy...")
ut.sortMAGFile("PaperFieldsOfStudy",["PaperId","FieldOfStudyId"],useSamstools=True)

print("Processing PaperMeSH...")
ut.sortMAGFile("PaperMeSH",["PaperId","DescriptorUI","QualifierUI"],useSamstools=True)

print("Processing PaperReferences...")
ut.sortMAGFile("PaperReferences",["PaperId"],useSamstools=True)

print("Processing PaperReferences By Reference...")
ut.sortMAGFile("PaperReferences",["PaperReferenceId"],useSamstools=True)

print("Processing Papers...")
ut.sortMAGFile("Papers",["PaperId"],useSamstools=True)


print("Processing Abstracts...")
ut.sortMAGFile("PaperAbstractsInvertedIndex",["PaperId"],useSamstools=True)

print("Processing Authors...")
ut.sortMAGFile("Authors",["AuthorId"],useSamstools=True)

print("Processing MESH Terms")
ut.sortMAGFile("PaperMeSH",["PaperId"],useSamstools=True)

# Create 
# Create merge and aggregate (some columns are aggregated)
# Filter function, create a new dbgz with only the requested columns

