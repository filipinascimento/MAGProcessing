import config as cfg
import utilities as ut
import dbgz
from tqdm.auto import tqdm

MAGPath = cfg.MAGPath
dataPath = cfg.dataPath
processedPath = cfg.processedPath
temporaryPath = cfg.temporaryPath


compressedMeSHFilename = ut.getMAGCompressedPath("PaperMeSH",["PaperId","DescriptorUI","QualifierUI"])
compressedPapersFilename = ut.getMAGCompressedPath("Papers",["PaperId"])


mesh2Description = {}
with open(processedPath/"PaperMeSHPapersData.tsv","wt") as fdOut:
    #header
    # paperIndex
    # paperEntry["PaperId"]
    # paperEntry["PaperTitle"]
    # paperEntry["Year"]
    # paperEntry["JournalId"]
    # meshData["mainMeshTerms"]
    # meshData["meshTerms"]

    fdOut.write("paperIndex\tPaperId\tPaperTitle\tYear\tJournalId\tmainMeshTerms\tmeshTerms\n")
    with dbgz.DBGZReader(compressedPapersFilename) as paperDataset:
        with dbgz.DBGZReader(compressedMeSHFilename) as meshDataset:
            
            meshData = {
                "mainMeshTerms" : [],
                "meshTerms" : [],
                "paperEntry" : None,
                "paperIndex" : 0,
                "paper2Index": {},
                "meshPaperID": -1
            }
            paperEntries = paperDataset.entries
            #PaperIndex PaperID Title Year VenueID MainMeshIDs MeshIDs
            def closeEntry(meshData,paperEntries,fdOut,paperTqdm):
                paperEntry = meshData["paperEntry"]
                if(paperEntry and 
                    paperEntry["PaperId"]==meshData["meshPaperID"]):
                    paperEntry = meshData["paperEntry"]
                else:
                    for paperEntry in paperEntries:
                        paperTqdm.update(1)
                        paperID = paperEntry["PaperId"]
                        meshData["paperEntry"] = paperEntry
                        if paperID >= meshData["meshPaperID"]:
                            break
                paperID = paperEntry["PaperId"]
                meshPaperID = meshData["meshPaperID"]
                if(paperID==meshPaperID):
                    paperIndex = meshData["paperIndex"]
                    meshData["meshPaperID"] = paperID
                    rowEntries = []
                    rowEntries.append(str(paperIndex))
                    rowEntries.append(str(paperEntry["PaperId"]))
                    rowEntries.append(str(paperEntry["PaperTitle"]))
                    rowEntries.append(str(paperEntry["Year"]))
                    rowEntries.append(str(paperEntry["JournalId"]))
                    rowEntries.append(";".join(meshData["mainMeshTerms"]))
                    rowEntries.append(";".join(meshData["meshTerms"]))
                    fdOut.write("\t".join(rowEntries)+"\n")
                    meshData["paper2Index"][paperID] = paperIndex
                    meshData["paperIndex"] += 1
                else:
                    print("Paper %d not found."%(meshData["meshPaperID"]))
                meshData["mainMeshTerms"] = []
                meshData["meshTerms"] = []
                
            paperTqdm = tqdm(total=paperDataset.entriesCount,desc="Papers Dataset")
            for meshEntry in tqdm(meshDataset.entries,total=meshDataset.entriesCount,desc="MeSH Dataset"):
                paperID = meshEntry["PaperId"]
                if paperID != meshData["meshPaperID"]:
                    if meshData["meshPaperID"] != -1:
                        closeEntry(meshData,paperEntries,fdOut,paperTqdm)
                    meshData["meshPaperID"] = paperID
                meshComponents = [meshEntry["DescriptorUI"]]
                if(meshEntry["QualifierUI"]):
                    meshComponents.append(meshEntry["QualifierUI"])
                meshterm = "-".join(meshComponents)
                if(meshEntry["DescriptorUI"]):
                    mesh2Description[meshEntry["DescriptorUI"]] = meshEntry["DescriptorName"]
                if(meshEntry["QualifierUI"]):
                    mesh2Description[meshEntry["QualifierUI"]] = meshEntry["QualifierName"]
                if(meshEntry["IsMajorTopic"] and meshEntry["IsMajorTopic"]=="True"):
                    meshData["mainMeshTerms"].append(meshterm)
                meshData["meshTerms"].append(meshterm)
                
            closeEntry(meshData,paperEntries,fdOut,paperTqdm)
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








