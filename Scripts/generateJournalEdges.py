import config as cfg
import utilities as ut
import dbgz
from tqdm.auto import tqdm

MAGPath = cfg.MAGPath
dataPath = cfg.dataPath
processedPath = cfg.processedPath
temporaryPath = cfg.temporaryPath


compressedJournalsFilename = ut.getMAGCompressedPath("Journals",["JournalId"])
compressedPapersFilename = ut.getMAGCompressedPath("Papers",["JournalId","Year"])
compressedAuthorsFilename = ut.getMAGCompressedPath("PaperAuthorAffiliations",["PaperId","AuthorId","AuthorSequenceNumber"])
compressedJournalAuthorYearFilename = ut.getMAGCompressedPath("JournalAuthorYear")

journalName2ID = {}
journalID2Name = {}
journalID2PaperCount = {}
ignoredJournalIDs = [
    41354064,# cheminform
]

with dbgz.DBGZReader(compressedJournalsFilename) as journalsDataset:
    for journalEntry in tqdm(journalsDataset.entries,total=journalsDataset.entriesCount,desc="Journals Dataset"):
        journalID = journalEntry["JournalId"]
        journalName = journalEntry["NormalizedName"]
        if(journalEntry['Issn'] and journalID not in ignoredJournalIDs):
            journalName2ID[journalName] = journalID
            journalID2Name[journalID] = journalName
            journalID2PaperCount[journalID] = int(journalEntry["PaperCount"])



outputSchema = [
    ("JournalId","u"),
    ("authorIdsYears","I"),
]

selectedJournals = [
    "journal of informetrics",
    "scientometrics"
]

selectedJournalIndices = [journalName2ID[journalName] for journalName in selectedJournals]

with dbgz.DBGZWriter(compressedJournalAuthorYearFilename,outputSchema) as journalAuthorsYearsDataset:
    #header
    # paperIndex
    # paperEntry["PaperId"]
    # paperEntry["PaperTitle"]
    # paperEntry["Year"]
    # paperEntry["JournalId"]
    # meshData["mainMeshTerms"]
    # meshData["meshTerms"]

    with dbgz.DBGZReader(compressedPapersFilename) as paperDataset:
        with dbgz.DBGZReader(compressedAuthorsFilename) as authorDataset:
            print(authorDataset.scheme)
            authorsPaperData = {
                "authors-years" : [],
                "paperEntry" : None,
                "authorPaperID": -1
            }
            paperEntries = paperDataset.entries
            #PaperIndex PaperID Title Year VenueID MainMeshIDs MeshIDs
            def closeEntry(authorsPaperData,paperEntries,journalAuthorsYearsDataset,paperTqdm):
                paperEntry = authorsPaperData["paperEntry"]
                if(paperEntry and 
                    paperEntry["PaperId"]==authorsPaperData["authorPaperID"]):
                    paperEntry = authorsPaperData["paperEntry"]
                else:
                    for paperEntry in paperEntries:
                        paperTqdm.update(1)
                        paperID = paperEntry["PaperId"]
                        authorsPaperData["paperEntry"] = paperEntry
                        if paperID >= authorsPaperData["authorPaperID"]:
                            break
                paperID = paperEntry["PaperId"]
                authorPaperID = authorsPaperData["authorPaperID"]
                if(paperID==authorPaperID):
                    authorsPaperData["authorPaperID"] = paperID
                    authorsPaperData["authorsYears"]
                    journalAuthorsYearsDataset.write(
                        journalId=paperEntry["JournalId"],
                        authorIdsYears=authorsPaperData["authorsYears"],
                    )
                else:
                    print("Paper %d not found."%(authorsPaperData["authorPaperID"]))
                authorsPaperData["authorsYears"] = []
                
            paperTqdm = tqdm(total=paperDataset.entriesCount,desc="Papers Dataset")
            for authorPaperEntry in tqdm(authorDataset.entries,total=authorDataset.entriesCount,desc="Authors Dataset"):
                paperID = authorPaperEntry["PaperId"]
                if paperID != authorsPaperData["authorPaperID"]:
                    if authorsPaperData["authorPaperID"] != -1:
                        closeEntry(authorsPaperData,paperEntries,journalAuthorsYearsDataset,paperTqdm)
                    authorsPaperData["authorPaperID"] = paperID
                authorsPaperData["authors-years"].append(authorPaperEntry)
                authorsPaperData["authors-years"].append(authorPaperEntry)
                
            closeEntry(authorsPaperData,paperEntries,journalAuthorsYearsDataset,paperTqdm)
            paperTqdm.update()
            paperTqdm.close()


with open(processedPath/"MeSHDescriptions.tsv","wt") as fdOut:
    #header
    fdOut.write("\t".join(["MeSHID","Description"])+"\n")
    for meshTerm,desc in mesh2Description.items():
        fdOut.write("%s\t%s\n"%(meshTerm,desc))


compressedReferencesFilename = ut.getMAGCompressedPath("PaperReferences",["PaperId"])
compressedCitationsFilename = ut.getMAGCompressedPath("PaperReferences",["PaperReferenceId"])


paper2Index = meshData["paper2Index"]
with open(processedPath/"PaperReferences.tsv","wt") as fdOut:
    with open(processedPath/"PaperReferencesExternal.tsv","wt") as fdOutExternal:
        fdOut.write("fromPaperIndex\ttoPaperIndex\n")
        fdOutExternal.write("fromPaperIndex\ttoPaperID\n")
        with dbgz.DBGZReader(compressedReferencesFilename) as paperDataset:
            for entry in tqdm(paperDataset.entries,total=paperDataset.entriesCount,desc="References Dataset"):
                paperID = entry["PaperId"]
                referenceID = entry["PaperReferenceId"]
                if(paperID in paper2Index):
                    fromIndex = paper2Index[paperID]
                    if(referenceID in paper2Index):
                        toIndex = paper2Index[referenceID]
                        fdOut.write("%d\t%d\n"%(fromIndex,toIndex))
                    else:
                        fdOutExternal.write("%d\t%d\n"%(fromIndex,referenceID))
                


paper2Index = meshData["paper2Index"]
with open(processedPath/"PaperCitations.tsv","wt") as fdOut:
    with open(processedPath/"PaperCitationsExternal.tsv","wt") as fdOutExternal:
        fdOut.write("toPaperIndex\tfromPaperIndex\n")
        fdOutExternal.write("toPaperIndex\tfromPaperID\n")
        with dbgz.DBGZReader(compressedCitationsFilename) as paperDataset:
            for entry in tqdm(paperDataset.entries,total=paperDataset.entriesCount,desc="Citations Dataset"):
                paperID = entry["PaperId"]
                referenceID = entry["PaperReferenceId"]
                if(referenceID in paper2Index):
                    fromIndex = paper2Index[referenceID]
                    if(paperID in paper2Index):
                        toIndex = paper2Index[paperID]
                        fdOut.write("%d\t%d\n"%(fromIndex,toIndex))
                    else:
                        fdOutExternal.write("%d\t%d\n"%(fromIndex,paperID))
                



compressedPaperAuthorsFilename = ut.getMAGCompressedPath("PaperAuthorAffiliations",["PaperId","AuthorId","AuthorSequenceNumber"])
compressedAuthorsPaperFilename = ut.getMAGCompressedPath("PaperAuthorAffiliations",["AuthorId","AffiliationId"])


paper2Index = meshData["paper2Index"]
with open(processedPath/"PaperIndexAuthorID.tsv","wt") as fdOut:
    fdOut.write("paperIndex\tauthorID\n")
    with dbgz.DBGZReader(compressedPaperAuthorsFilename) as paperDataset:
        for entry in tqdm(paperDataset.entries,total=paperDataset.entriesCount,desc="Authors Dataset"):
            paperID = entry["PaperId"]
            authorID = entry["AuthorId"]
            if(paperID in paper2Index):
                paperIndex = paper2Index[paperID]
                fdOut.write("%d\t%d\n"%(paperIndex,authorID))


paper2Index = meshData["paper2Index"]
with open(processedPath/"AuthorIDPaperIndex.tsv","wt") as fdOut:
    fdOut.write("authorID\tpaperIndex\n")
    with dbgz.DBGZReader(compressedAuthorsPaperFilename) as paperDataset:
        for entry in tqdm(paperDataset.entries, total=paperDataset.entriesCount, desc="Authors2Papers Dataset"):
            paperID = entry["PaperId"]
            authorID = entry["AuthorId"]
            if(paperID in paper2Index):
                paperIndex = paper2Index[paperID]
                fdOut.write("%d\t%d\n"%(authorID,paperIndex))





###REPEAT








