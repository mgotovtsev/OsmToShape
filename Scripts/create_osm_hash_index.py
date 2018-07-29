#BUILTIN------------------------------------------------------------------------
import os
import sys
import struct
import math
import random
import re
import datetime
import time
import codecs
import copy
import fnmatch
import collections
import unicodedata
import traceback
#GDAL---------------------------------------------------------------------------
import gdal
import ogr
import osr
import gdalnumeric
import gdalconst
#PROJECT------------------------------------------------------------------------
from hash_table_hdd      import *
from parameters    import *
from procedures   import *

"""
TODO: 1) standartization print messages
      2) add comments
"""

def CountNodeWayRelation(sOsmFilePath):
    """
    Get count Node, Way, Relation from osm
    TODO: 1)define block size; 2) check dellimiter that is \n
    """
    OsmFileForCount = open(sOsmFilePath, 'rb')
    nBlockSize = 250 * (1024 ** 2) # (1024 ** 2) - MegaByte

    sOsmFileBlock = OsmFileForCount.read(nBlockSize)

    nNode, nWay, nRelation = 0, 0, 0

    while sOsmFileBlock:

        nNode     += sOsmFileBlock.count(sNodePatternForGetCount)
        nWay      += sOsmFileBlock.count(sWayPatternForGetCount)
        nRelation += sOsmFileBlock.count(sRelationPatternForGetCount)

        sOsmFileBlock = OsmFileForCount.read(nBlockSize)

    OsmFileForCount.close()

    return nNode, nWay, nRelation

def FillHashTables(sOsmPath):
    """
    Create and fill hdd hash table with osm key data
    TODO: 1) define temp path for result hash table; 2) get only required index node, way, relation
    """
    try:
        sMessage = 'Get OSM objects count'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        timeStartFillHashTablesTime = datetime.datetime.today()

        nNodeCnt, nWayCnt, nRelationCnt = CountNodeWayRelation(sOsmPath)
        ##nNodeCnt, nWayCnt, nRelationCnt = 691928482, 45799036, 380539

        sMessage = '\tNode count: %s'     % nNodeCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        sMessage = '\tWay count: %s'      % nWayCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        sMessage = '\tRelation count: %s' % nRelationCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMessage = 'Step time: %s' % (datetime.datetime.today() - timeStartFillHashTablesTime)
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMessage = 'Fill hash tables'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        NodeHshTbl     = HddHashTbl('NodeHshTbl',     nNodeCnt, 'qdd')
        WayHshTbl      = HddHashTbl('WayHshTbl',      nWayCnt)
        RelationHshTbl = HddHashTbl('RelationHshTbl', nRelationCnt)

        NodeHshTbl.CreateHshTblFile()
        WayHshTbl.CreateHshTblFile()
        RelationHshTbl.CreateHshTblFile()

        OsmFile      = open(sOsmPath, 'rb')

        nOffset      = 0
        OsmFile.seek(nOffset)
        sCurrOsmLine = OsmFile.readline()

        nNodeCnt, nWayCnt, nRelationCnt, nRelationWaysCnt = 0, 0, 0, 0

        timeStartReadTime = datetime.datetime.today()

        listNodeWayRelationState = [0, 0, 0]

        while sCurrOsmLine:

            #Processing Node and Way
            if listNodeWayRelationState == [1, 0, 0]:
                NodeFindResult = re.search(NodePattern, sCurrOsmLine)

                if NodeFindResult and len(NodeFindResult.groups()) > 0:
                    nNodeId = int(NodeFindResult.groups()[0])

                    NodeHshTbl[nNodeId] = ParseCoordinates(sCurrOsmLine)
                    nNodeCnt += 1

                    if nNodeCnt % 1000000 == 0:
                        sMessage = '\tNode insert into hash table: %s \t %s' % (nNodeCnt, (datetime.datetime.today() - timeStartReadTime))
                        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                        timeStartReadTime = datetime.datetime.today()

                else:
                    WayFindResult  = re.search(WayPattern, sCurrOsmLine)
                    if WayFindResult and len(WayFindResult.groups()) > 0:

                        nWayId = int(WayFindResult.groups()[0])

                        WayHshTbl[nWayId] = (nOffset,)
                        nWayCnt += 1

                        if nWayCnt % 1000000 == 0:
                            sMessage = '\tWay insert into hash table: %s \t %s' % (nWayCnt, (datetime.datetime.today() - timeStartReadTime))
                            PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                            timeStartReadTime = datetime.datetime.today()

                        listNodeWayRelationState = [1, 1, 0]
                        WayHshTbl.SetOffsetForRead(nOffset)

                        sMessage = 'Start fill ways'
                        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

            # Processing Way and Relation
            elif listNodeWayRelationState == [1, 1, 0]:
                WayFindResult  = re.search(WayPattern, sCurrOsmLine)
                if WayFindResult and len(WayFindResult.groups()) > 0:

                    nWayId = int(WayFindResult.groups()[0])

                    WayHshTbl[nWayId] = (nOffset,)
                    nWayCnt += 1

                    if nWayCnt % 1000000 == 0:
                        sMessage = '\tWay insert into hash table: %s \t %s' % (nWayCnt, (datetime.datetime.today() - timeStartReadTime))
                        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                        timeStartReadTime = datetime.datetime.today()

                else:
                    RelFindResult  = re.search(RelPattern, sCurrOsmLine)
                    if RelFindResult and len(RelFindResult.groups()) > 0:

                        nRelId = int(RelFindResult.groups()[0])

                        RelationHshTbl[nRelId] = (nOffset,)
                        nRelationCnt += 1

                        if nRelationCnt % 1000000 == 0:
                            sMessage = '\tRelation insert into hash table: %s \t %s' % (nRelationCnt, (datetime.datetime.today() - timeStartReadTime))
                            PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                            timeStartReadTime = datetime.datetime.today()

                        listNodeWayRelationState = [1, 1, 1]
                        RelationHshTbl.SetOffsetForRead(nOffset)

                        sMessage = 'Start fill relations'
                        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

            # Processing Relation
            elif listNodeWayRelationState == [1, 1, 1]:
                RelFindResult  = re.search(RelPattern, sCurrOsmLine)
                if RelFindResult and len(RelFindResult.groups()) > 0:

                    nRelId = int(RelFindResult.groups()[0])

                    RelationHshTbl[nRelId] = (nOffset,)
                    nRelationCnt += 1

                    if nRelationCnt % 1000000 == 0:
                        sMessage = '\Relation insert into hash table: %s \t %s' % (nRelationCnt, (datetime.datetime.today() - timeStartReadTime))
                        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                        timeStartReadTime = datetime.datetime.today()

                MemberSearchResult  = re.search(RelMemberPattern, sCurrOsmLine)
                if MemberSearchResult and len(MemberSearchResult.groups()) > 0:
                    sMemberType = MemberSearchResult.groups()[0]
                    sMemberRole = MemberSearchResult.groups()[2]

                    if (sMemberType.lower() == objOsmMainTags.sWay
                       and sMemberRole.upper() in (objOsmPolygonRole.sOuter, objOsmPolygonRole.sInner)):
                       nRelationWaysCnt += 1

            #Processing Node only
            elif listNodeWayRelationState == [0, 0, 0]:
                NodeFindResult = re.search(NodePattern, sCurrOsmLine)

                if NodeFindResult and len(NodeFindResult.groups()) > 0:
                    nNodeId = int(NodeFindResult.groups()[0])

                    NodeHshTbl[nNodeId] = ParseCoordinates(sCurrOsmLine)
                    nNodeCnt += 1

                    listNodeWayRelationState = [1, 0, 0]
                    NodeHshTbl.SetOffsetForRead(nOffset)

                    sMessage = 'Start fill nodes'
                    PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

            nOffset += len(sCurrOsmLine)
            sCurrOsmLine = OsmFile.readline()

        sMessage = 'Fill Node count: %s'     % nNodeCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        sMessage = 'Fill Way count: %s'      % nWayCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        sMessage = 'Fill Relation count: %s' % nRelationCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
        sMessage = 'Fill Outer/Inner Ways in relations count: %s' % nRelationWaysCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        OsmFile.close()

        RelationWaysHshTbl = HddHashTbl('RelationWaysHshTbl', nRelationWaysCnt, 'qq')
        RelationWaysHshTbl.CreateHshTblFile()

        NodeHshTbl.FreeHshTbl()
        WayHshTbl.FreeHshTbl()
        RelationHshTbl.FreeHshTbl()
        RelationWaysHshTbl.FreeHshTbl()

        sMessage = 'Fill Hash Tables finish %s' % (datetime.datetime.today() - timeStartFillHashTablesTime)
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
    except:
        sMessage = 'Error in FillHashTables'
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

def ReadFileByBlock(InputFileOrPath, nBlockSize, sBorderMarker = u'\n', nInitSeek = 0):
    try:

        if isinstance(InputFileOrPath, (type(''), type(u''))):
           InputFile = codecs.open(InputFileOrPath, 'rb', 'utf-8')
        else:
           InputFile = InputFileOrPath

        if nInitSeek <> 0:
           InputFile.seek(nInitSeek)

        sBuffer         = None
        sAdditionalData = None

        while sBuffer <> u'':
            # Read block
            sBuffer = InputFile.read(nBlockSize)

            while (not sBuffer.endswith(sBorderMarker)) and (sAdditionalData <> u''):
                # Read next byte
                sAdditionalData = InputFile.read(1)
                sBuffer += sAdditionalData

            yield sBuffer

        InputFile.close()

    except:
        pass
    finally:
        if 'InputFile' in locals() and not InputFile.closed: InputFile.close()

def OsmReaderFileGenerator(OsmFile, nInitSeek, sClosedTag = '/>'):
    try:

        genOsmFile = ReadFileByBlock(OsmFile, nBlockSize, sBorderMarker = sClosedTag, nInitSeek = nInitSeek)

        for sBlock in genOsmFile:
            sBlockSplitted = sBlock.split('\n')
            del sBlock
            for sOsmLine in sBlockSplitted:
                yield sOsmLine#.encode('utf-8')

    except:
        pass
    finally:
        if 'genOsmFile' in locals(): del genOsmFile

def ReadFromGen(Generator):
    for sCurrReadLine in Generator:
        return sCurrReadLine
    else:
        return None

def OsmFileReaderGen(OsmFile):
    OsmFileGen = (sCurrReadLine.decode('utf-8') for sCurrReadLine in OsmFile.xreadlines())

    for sCurrReadLine in OsmFileGen:

        if sCurrReadLine:
           listLines = []
           listLines.append(sCurrReadLine)
        else:
            break

        for i in xrange(100000):
            sCurrReadLine = ReadFromGen(OsmFileGen)
            if sCurrReadLine:
               listLines.append(sCurrReadLine)
            else:
                break

        if len(listLines) > 0:
           for sCurrReadLine in listLines:
               yield sCurrReadLine

def ParseOsmBlockIntoBinHashTable(BinHashTable, sPatternForGetOsmObjInfo, sBlock, nCurrentOffset, reSearchPattern, isFirstOsmFeatureTypeForRead):
    nStartPosSearch  = 0
    nFinishPosSearch = len(sBlock)
    nLenOfEndReadKey = len(sPatternForGetOsmObjInfo)

    nStartPosSearch  = sBlock.find(sPatternForGetOsmObjInfo, nStartPosSearch, nFinishPosSearch)
    nNextPosSearch   = sBlock.find('>', nStartPosSearch + nLenOfEndReadKey, nFinishPosSearch)

    # Write offset to way or relation section
    if isFirstOsmFeatureTypeForRead == True:
       isFirstOsmFeatureTypeForRead = False
       nObjOffset = nCurrentOffset - (nFinishPosSearch - nStartPosSearch)
       BinHashTable.nOffsetForRead = nObjOffset

    # Search way (or relation) id and offset and fill each way (or relation) into bin hash table
    while nStartPosSearch > -1:

        sOsmObjXmlHeader       = sBlock[nStartPosSearch : nNextPosSearch]

        nObjOffset = nCurrentOffset - (nFinishPosSearch - nStartPosSearch)

        listOsmIdFounded = re.findall(reSearchPattern, sOsmObjXmlHeader)

        if len(listOsmIdFounded) > 0:
            sOsmId = listOsmIdFounded[0]
            BinHashTable.SetValue(long(sOsmId), (nObjOffset,))
        else:
            sMessage = 'Error with fill into bin hash table!'
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
            raise SystemExit

        nStartPosSearch = nNextPosSearch

        nStartPosSearch = sBlock.find(sPatternForGetOsmObjInfo, nStartPosSearch, nFinishPosSearch)
        nNextPosSearch  = sBlock.find('>', nStartPosSearch + nLenOfEndReadKey, nFinishPosSearch)

    return isFirstOsmFeatureTypeForRead

def ParseCoordinates(sNodeDefTag):
    """
    Get float coordinates from node tag
    """
    try:

        NodeLonResult = re.search(LonPattern, sNodeDefTag)
        if NodeLonResult and len(NodeLonResult.groups()) > 0:
            fNodeLon = float(NodeLonResult.groups()[0])

        NodeLatResult = re.search(LatPattern, sNodeDefTag)
        if NodeLatResult and len(NodeLatResult.groups()) > 0:
            fNodeLat = float(NodeLatResult.groups()[0])

        return ( fNodeLon, fNodeLat)
    except:
        sMessage = 'Error in ParseCoordinates in sNodeDefTag = %s' % sNodeDefTag
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)

def GetWayCoordinates(NodeHashTbl, listNodesIds, nFeatureId):
    """
       Get coordinates for list of Nodes
    """

    listCoordinates = []

    if len(listNodesIds) == 0:
        sMessage = 'WayId %s have incorrect geometry. Not exist any nodes.' % nFeatureId
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        return listCoordinates

    for nSearchNodeId in listNodesIds:

        tplNodeCoords = NodeHashTbl[nSearchNodeId]

        if tplNodeCoords in [(0, 0), None]:
            sMessage = 'Node Id %s not found for Way Id %s. This Node will be skipped!' % (nSearchNodeId, nFeatureId)
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        else:
            listCoordinates.append(tplNodeCoords)

    return listCoordinates

def ReadNextNode(OsmFile):
    """
    Read each node from osm file and return (yeld) nNodeId, tplCoordinates, dictAttributes
    """
    dictEmptyAttributes = {}

    OsmFileGenerator = (sCurrReadLine for sCurrReadLine in OsmFile.xreadlines())
    for sCurrReadLine in OsmFileGenerator:

        if sCurrReadLine.count('<node') > 0:

            NodeFindResult = re.search(SimpleNodePattern, sCurrReadLine)

            if NodeFindResult and len(NodeFindResult.groups()) > 0:
                nNodeId = int(NodeFindResult.groups()[0])

                tplCoordinates = ParseCoordinates(sCurrReadLine)

                yield nNodeId, tplCoordinates, dictEmptyAttributes

            else:

                NodeFindResult = re.search(AttributeNodePattern, sCurrReadLine)

                if NodeFindResult and len(NodeFindResult.groups()) > 0:

                    nNodeId = int(NodeFindResult.groups()[0])

                    tplCoordinates = ParseCoordinates(sCurrReadLine)

                    dictAttributes = {}

                    for sCurrReadLine in OsmFileGenerator:

                        if sCurrReadLine.count('</node>') > 0:
                            break
                        else:
                            AttrFindResult = re.search(KeyValuePattern, sCurrReadLine)

                            if AttrFindResult and len(AttrFindResult.groups()) > 0:
                                dictAttributes[AttrFindResult.groups()[0]] = AttrFindResult.groups()[1]

                    yield nNodeId, tplCoordinates, dictAttributes

        elif sCurrReadLine.count('<way') > 0 or sCurrReadLine.count('<relation') > 0:
            break

def ReadNextWay(OsmFile, WayHashTbl, NodeHashTbl):
    """
    Read each way from osm file and return (yeld) nWayId, listCoordinates, dictAttributes
    """
    nWaySeek = WayHashTbl.nOffsetForRead

    OsmFile.seek(nWaySeek)

    OsmFileGenerator = (sCurrReadLine for sCurrReadLine in OsmFile.xreadlines())

    dictEmptyAttributes = {}

    for sCurrReadLine in OsmFileGenerator:
        nWaySeek += len(sCurrReadLine)
        # Search way tag
        if sCurrReadLine.count('<way') > 0:

            WayFindResult = re.search(WayPattern, sCurrReadLine)
            # Get Way Id
            if WayFindResult and len(WayFindResult.groups()) > 0:

                nWayId = int(WayFindResult.groups()[0])

                # Get Way points IDs
                listNodesIds  = []
                for sCurrReadLine in OsmFileGenerator:
                    nWaySeek += len(sCurrReadLine)
                    if sCurrReadLine.count('<nd ref="') > 0:

                        NdLinkSearchResult = re.search(NdRefPattern, sCurrReadLine)

                        if NdLinkSearchResult and len(NdLinkSearchResult.groups()) > 0:

                            nNodeId = int(NdLinkSearchResult.groups()[0])
                            listNodesIds.append(nNodeId)

                    else:
                        break

                if sCurrReadLine.count('</way>') > 0:
                    listCoordinates = GetWayCoordinates(NodeHashTbl, listNodesIds, nWayId)
                    yield nWayId, listCoordinates, dictEmptyAttributes
                    continue

                # Get Way attributes
                ## TODO: Read attribute is can be separate function (decomposition)
                dictAttributes = {}

                AttrFindResult = re.search(KeyValuePattern, sCurrReadLine)

                if AttrFindResult and len(AttrFindResult.groups()) > 0:
                    dictAttributes[AttrFindResult.groups()[0]] = AttrFindResult.groups()[1]
                else:
                    print 'In way %s something wrong with attributes' % (nWayId)
                    yield nWayId, (), dictEmptyAttributes
                    continue

                for sCurrReadLine in OsmFileGenerator:
                    nWaySeek += len(sCurrReadLine)
                    if sCurrReadLine.count('</way>') > 0:
                        break
                    else:
                        AttrFindResult = re.search(KeyValuePattern, sCurrReadLine)

                        if AttrFindResult and len(AttrFindResult.groups()) > 0:
                            dictAttributes[AttrFindResult.groups()[0]] = AttrFindResult.groups()[1]
                        else:
                            print 'In way %s something wrong with attributes' % (nWayId)
                            yield nWayId, (), dictAttributes
                            break

                # Get line geometry
                listCoordinates = GetWayCoordinates(NodeHashTbl, listNodesIds, nWayId)
                yield nWayId, listCoordinates, dictAttributes
        elif sCurrReadLine.count('<relation') > 0:
            break


def GetNodeListFromWay(OsmFile, WayHashTbl, NodeHashTbl, listWayIds, nOldOffset, nRelationId = 0):
    listWayCoodinatesLists = []

    for nWayId in listWayIds:

        nWaySeek = WayHashTbl[nWayId]

        if not nWaySeek:
            sRelationMessage = ' in Relation %s' % nRelationId if nRelationId <> 0 else ''
            sMessage = 'Wrong link for way %s' % (nWayId) + sRelationMessage

            if nRelationId == 0:
               PrintMessageToLog(objLogPaths.sCreateLnGeomErrorsLogPath, sMessage)
            else:
               PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

            continue

        nWaySeek = nWaySeek[0]

        OsmFile.seek(nWaySeek)

        OsmFileGenerator = (sCurrReadLine for sCurrReadLine in OsmFile.xreadlines())

        for sCurrReadLine in OsmFileGenerator:
            nWaySeek += len(sCurrReadLine)

            # Search way tag
            if sCurrReadLine.count('<way') > 0:
               break

        # Get Way points IDs
        listNodesIds  = []

        for sCurrReadLine in OsmFileGenerator:
            nWaySeek += len(sCurrReadLine)

            if sCurrReadLine.count(sNdRefPattern) > 0:

                NdLinkSearchResult = re.search(NdRefPattern, sCurrReadLine)

                if NdLinkSearchResult and len(NdLinkSearchResult.groups()) > 0:

                    nNodeId = int(NdLinkSearchResult.groups()[0])
                    listNodesIds.append(nNodeId)

            else:
                break


        listCoordinates = GetWayCoordinates(NodeHashTbl, listNodesIds, nWayId)

        listWayCoodinatesLists.append(listCoordinates)

    OsmFile.seek(nOldOffset)

    return listWayCoodinatesLists
