# -*- coding: utf-8 -*-
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
import xml.sax.saxutils # unescape
import xml.etree.ElementTree as ElementTree
#GDAL---------------------------------------------------------------------------
import gdal
import ogr
import osr
import gdalnumeric
import gdalconst
#PROJECT------------------------------------------------------------------------
from parameters      import * # TBD. Fix this poor codestyle
from procedures     import *
from hash_table_hdd        import *
from create_osm_hash_index import *


import cProfile


def profile(func):
    """Decorator for run function profile"""
    def wrapper(*args, **kwargs):
        profile_filename = func.__name__ + '.prof'
        profiler = cProfile.Profile()
        result = profiler.runcall(func, *args, **kwargs)
        profiler.dump_stats(profile_filename)
        return result
    return wrapper



gdal.SetConfigOption('SHAPE_2GB_LIMIT', 'TRUE')
##gdal.SetConfigOption('ENCODING', 'UTF-8')
#-------------------------------------------------------------------------------

def ReadXMLParemeters(sXMLFilePath):
    """
       Function for read parameters from XML file
    """
    try:
        # <check input parameters>

        if not os.path.exists(sXMLFilePath):
            sMessage = 'Xml file not exists!'
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
            raise SystemExit # Exception

        # </check input parameters>

        dictReferences     = collections.OrderedDict() # References section
        dictFieldPatterns  = collections.OrderedDict() # Field_patterns section
        dictFilterCond     = collections.OrderedDict() # Filter_conditions section

        RootTree = ElementTree.parse(sXMLFilePath) # Exception
        RootNode = RootTree.getroot() # Exception

        for Field_patterns in RootNode.iter('Field_patterns'): # Exception
            for Pattern in Field_patterns.findall('Pattern'): # Exception
                sPatternName = Pattern.get('name') # Exception
                dictFieldPatterns[sPatternName] = []

                for PatternField in Pattern.findall('Field'): # Exception

                    dictFieldPatterns[sPatternName].append([])

                    sTagName            = PatternField.get('TagName')
                    sFieldName          = PatternField.get('FieldName')
                    sPriority           = int(PatternField.get('Priority'))
                    sFieldType          = PatternField.get('FieldType')
                    sFieldTypeLength    = int(PatternField.get('FieldTypeLength'))
                    sFieldTypeScale     = int(PatternField.get('FieldTypeScale'))
                    sFieldTypePrecision = int(PatternField.get('FieldTypePrecision'))
                    sFieldFormula       = PatternField.get('FieldFormula')

                    dictFieldPatterns[sPatternName][-1].append(sTagName)
                    dictFieldPatterns[sPatternName][-1].append(sFieldName)
                    dictFieldPatterns[sPatternName][-1].append(sPriority)
                    dictFieldPatterns[sPatternName][-1].append(sFieldType)
                    dictFieldPatterns[sPatternName][-1].append(sFieldTypeLength)
                    dictFieldPatterns[sPatternName][-1].append(sFieldTypeScale)
                    dictFieldPatterns[sPatternName][-1].append(sFieldTypePrecision)
                    dictFieldPatterns[sPatternName][-1].append(sFieldFormula)

        for Filter_conditions in RootNode.iter('Filter_conditions'): # Exception
            for Condition in Filter_conditions.findall('Condition'): # Exception
                sConditionName = Condition.get('name') # Exception
                dictFilterCond[sConditionName] = []
                for Filter in Condition.findall('Filter'): # Exception
                    sTagName  = Filter.get('tag_name')  # Exception
                    sTagValue = Filter.get('tag_value') # Exception
                    dictFilterCond[sConditionName].append([sTagName, sTagValue])

        for Relation in RootNode.iter('Relation'):          # Exception
            sLayerName    = Relation.get('LayerName')       # Exception
            sLayerType    = Relation.get('LayerType')       # Exception
            sFieldPattern = Relation.get('FieldPattern')    # Exception
            sCondition    = Relation.get('Condition')       # Exception
            if dictReferences.has_key(sLayerType):
                dictReferences[sLayerType].append([sLayerName,
                                              dictFieldPatterns[sFieldPattern],
                                              dictFilterCond[sCondition]])
            else:
                dictReferences[sLayerType] = [[sLayerName,
                                              dictFieldPatterns[sFieldPattern],
                                              dictFilterCond[sCondition]]]
    except:
        sMessage = traceback.format_exc()
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        raise SystemExit # Exception

    return dictReferences

#-------------------------------------------------------------------------------

def ReadAttributes(sCurrReadLine, OsmFileGenerator, sOsmFeatureType, nFeatureId, dictConditionIndex, sFeatureType, sCurrCloseFeatureTag, nCurrSeek = 0):

    # Get Way attributes

    dictAttributes     = {}
    setLayersForInsert = set() # set of layer names for write row

    if sCurrReadLine.count(sCurrCloseFeatureTag) > 0:
       return setLayersForInsert, dictAttributes, nCurrSeek

    AttrFindResult = re.search(KeyValuePattern, sCurrReadLine)

    if AttrFindResult and len(AttrFindResult.groups()) > 0:

        sTagName = AttrFindResult.groups()[0].strip()
        sValue   = xml.sax.saxutils.unescape(AttrFindResult.groups()[1].strip(), dictHtmlSpecSymbolsMapping)
        dictAttributes[sTagName] = sValue #AttrFindResult.groups()[1]

        # Prepare key_value condition for match with condition list
        sCurrStrCondition = (u'%s_' % (sTagName)).upper()
        if sCurrStrCondition in dictConditionIndex[sFeatureType]:
            # Get matched layer name and add to set
            sCurrLayerForInsert = dictConditionIndex[sFeatureType][sCurrStrCondition][0]
            setLayersForInsert.add(sCurrLayerForInsert)
        else:
            # This give layer name for write line
            sCurrStrCondition = (u'%s_%s' % (sTagName, sValue)).upper()
            if sCurrStrCondition in dictConditionIndex[sFeatureType]:
                # Get matched layer name and add to set
                sCurrLayerForInsert = dictConditionIndex[sFeatureType][sCurrStrCondition][0]
                setLayersForInsert.add(sCurrLayerForInsert)

    else:
        sMessage = 'In %s %s something wrong with attributes' % (sOsmFeatureType, nFeatureId)
        PrintMessageToLog(objLogPaths.sAttributesErrorsLogPath, sMessage)
        return setLayersForInsert, dictAttributes, nCurrSeek

    for sCurrReadLine in OsmFileGenerator:
        nCurrSeek += len(sCurrReadLine.encode('utf-8'))

        if sCurrReadLine.count(sCurrCloseFeatureTag) > 0:
            break
        else:
            sCurrReadLine = sCurrReadLine
            AttrFindResult = re.search(KeyValuePattern, sCurrReadLine)

            if AttrFindResult and len(AttrFindResult.groups()) > 0:
                sTagName = AttrFindResult.groups()[0].strip()
                sValue   = xml.sax.saxutils.unescape(AttrFindResult.groups()[1].strip(), dictHtmlSpecSymbolsMapping)
                dictAttributes[sTagName] = sValue #AttrFindResult.groups()[1]

                # Prepare key_value condition for match with condition list
                sCurrStrCondition = (u'%s_' % (sTagName)).upper()
                if sCurrStrCondition in dictConditionIndex[sFeatureType]:
                    # Get matched layer name and add to set
                    sCurrLayerForInsert = dictConditionIndex[sFeatureType][sCurrStrCondition][0]
                    setLayersForInsert.add(sCurrLayerForInsert)
                else:
                    # This give layer name for write line
                    sCurrStrCondition = (u'%s_%s' % (sTagName, sValue)).upper()
                    if sCurrStrCondition in dictConditionIndex[sFeatureType]:
                        # Get matched layer name and add to set
                        sCurrLayerForInsert = dictConditionIndex[sFeatureType][sCurrStrCondition][0]
                        setLayersForInsert.add(sCurrLayerForInsert)
            else:
                sMessage = 'In %s %s something wrong with attributes' % (sOsmFeatureType, nFeatureId)
                PrintMessageToLog(objLogPaths.sAttributesErrorsLogPath, sMessage)
                break

    return setLayersForInsert, dictAttributes, nCurrSeek

#-------------------------------------------------------------------------------

def SplitShapeFiles(dictLayersForInsert, sFeatureType, sLayersForInsert, geomCurrFeature):

    if gdal.GetLastErrorMsg() == '':
        return dictLayersForInsert

    sShapeMoreTwoGbMessage = 'Error in psSHP->sHooks.Fwrite() while writing object to .shp file.'
    sDbfMoreTwoGbMessage   = '2GB file size limit reached for'
    sDbfFailureWrite       = 'Failure writing DBF record'

    if (gdal.GetLastErrorMsg().find(sShapeMoreTwoGbMessage)  == 0
        or gdal.GetLastErrorMsg().find(sDbfMoreTwoGbMessage) == 0
        or gdal.GetLastErrorMsg().find(sDbfFailureWrite)     == 0):

        sSourceShapePath = dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetName()

        sSourceShapeDir     = os.path.dirname(sSourceShapePath)
        sSourceShapeCounter = os.path.basename(sSourceShapePath).split('.')[0].split('_')[-1]

        nSourceShapeCounter = int(sSourceShapeCounter) + 1 if sSourceShapeCounter.isdigit() else 1
        if nSourceShapeCounter == 1:
          sSourceShapeName = '_'.join(os.path.basename(sSourceShapePath).split('.')[0].split('_'))
        else:
          sSourceShapeName = '_'.join(os.path.basename(sSourceShapePath).split('.')[0].split('_')[:-1])

        sSourceShapeCounter = str(nSourceShapeCounter).zfill(3)

        sDestShapePath = os.path.join(sSourceShapeDir, '%s_%s.shp' % (sSourceShapeName, sSourceShapeCounter))

        CopyEmptyShapeFile(sSourceShapePath, sDestShapePath)

        dictLayersForInsert[sFeatureType][sLayersForInsert][0].Destroy()
        dictLayersForInsert[sFeatureType][sLayersForInsert][0] = ogr.Open(sDestShapePath, update = 1)
        dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).CreateFeature(geomCurrFeature)

        if (gdal.GetLastErrorMsg().find(sShapeMoreTwoGbMessage) == 0
            or gdal.GetLastErrorMsg().find(sDbfMoreTwoGbMessage) == 0
            or gdal.GetLastErrorMsg().find(sDbfFailureWrite) == 0):
            sMessage = '2Gb shapefile error with split function'
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
            raise SystemExit

    return dictLayersForInsert

#-------------------------------------------------------------------------------

def SetAttributeToFeature(geomFeature, dictLayersForInsert, listFields, dictAttributes, sFeatureType, sLayersForInsert, sOsmFeatureIdName, nOsmFeatureIdValue):
    try:
        featureDefn = dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).GetLayerDefn()
        geomCurrFeature = ogr.Feature(featureDefn)
        geomCurrFeature.SetGeometry(geomFeature)

        # Create attributes
        dictFieldsToTagMapping = listFields[1] # Field to Tags by priority

        for sInsertField in dictFieldsToTagMapping:
            # Define required tag value by priority
            for sCurrSrcTag in dictFieldsToTagMapping[sInsertField]:
                sTagValue = dictAttributes.get(sCurrSrcTag, None)

                # Value is found
                if sTagValue:
                   break

            if not sTagValue:
                continue

            if len(sTagValue) == len(sTagValue.encode('utf-8')):

                if len(sTagValue) > 254:
                   sTagValue = sTagValue[:254]

                geomCurrFeature.SetField(sInsertField, str(sTagValue))
            else:
                sTagValue = sTagValue.encode('utf-8')

                if len(sTagValue) > 254:
                    sTagValue = sTagValue[:254]

                geomCurrFeature.SetField2(sInsertField, sTagValue)

        geomCurrFeature.SetField(sOsmFeatureIdName, '%d' % (nOsmFeatureIdValue))


    except:
        sMessage = traceback.format_exc()
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        raise SystemExit

    return geomCurrFeature

#-------------------------------------------------------------------------------

def FillHashTablesNades():
    try:

        sMessage = 'Fill bin hash tables for %s' % (os.path.basename(sOsmPath))
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        NodeHshTbl    = BinHddHashTbl(*objBinHashTablesParamsCreation.sNodeHshTbl)

        ConvertTypes  = lambda x: (long(x[0]), float(x[2]), float(x[1]))

        # Start fill nodes
        for sBlock in ReadFileByBlock(sOsmPath, nBlockSize):
            NodeHshTbl.SetListValues(map(ConvertTypes, re.findall(NodeFullPattern, sBlock)))

            # Break if way exists
            if sWayPatternForGetCount in sBlock:
                break

            # Break if relation exists
            if sRelationPatternForGetCount in sBlock:
                break

        nNodeCnt = NodeHshTbl.TotalValuesCnt
        sMessage = 'Filled Node count: %s'     % nNodeCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        del NodeHshTbl

    except:
        sMessage = 'Error in FillHashTablesNades'
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

#-------------------------------------------------------------------------------

def FillBinHashTablesWayRelation():
    try:

        sMessage = 'Fill bin hash tables for %s' % (os.path.basename(sOsmPath))
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        WayHshTbl      = BinHddHashTbl(*objBinHashTablesParamsCreation.sWayHshTbl)
        RelationHshTbl = BinHddHashTbl(*objBinHashTablesParamsCreation.sRelationHshTbl)

        nCurrentOffset    = 0
        nRelationWaysCnt  = 0
        isFirstWayForRead = True
        isFirstRelForRead = True

        # Start fill way / relations
        for sBlock in ReadFileByBlock(sOsmPath, nBlockSize):

            sBlock = sBlock.encode('utf-8')
            nCurrentOffset += len(sBlock)

            # Start fill ways
            if sWayPatternForGetCount in sBlock:
                isFirstWayForRead = ParseOsmBlockIntoBinHashTable(WayHshTbl, sWayPatternForGetCount, sBlock, nCurrentOffset, WayFullPattern, isFirstWayForRead)

            # Start fill relations
            if sRelationPatternForGetCount in sBlock:
                isFirstRelForRead = ParseOsmBlockIntoBinHashTable(RelationHshTbl, sRelationPatternForGetCount, sBlock, nCurrentOffset, RelFullPattern, isFirstRelForRead)

            if isFirstRelForRead == False:
                MemberSearchResult  = re.findall(RelMemberFullPattern, sBlock)
                nRelationWaysCnt += len(MemberSearchResult)

        nWayCnt = WayHshTbl.TotalValuesCnt
        sMessage = 'Filled Way count: %s'      % nWayCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        nRelationCnt = RelationHshTbl.TotalValuesCnt
        sMessage = 'Filled Relation count: %s' % nRelationCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMessage = 'Outer/Inner Ways in relations count: %s' % nRelationWaysCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        del WayHshTbl
        del RelationHshTbl

        objBinHashTablesParamsCreation.sRelationWaysHshTbl = objBinHashTablesParamsCreation.sRelationWaysHshTbl + (nRelationWaysCnt,)
        RelationWaysHshTbl = HddHashTbl(*objBinHashTablesParamsCreation.sRelationWaysHshTbl)
        RelationWaysHshTbl.CreateHshTblFile()
        RelationWaysHshTbl.FreeHshTbl()

    except:
        sMessage = 'Error in FillBinHashTablesWayRelation'
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

#-------------------------------------------------------------------------------

def FillBinHashTables():
    try:

        sMessage = 'Fill bin hash tables for %s' % (os.path.basename(sOsmPath))
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        NodeHshTbl     = BinHddHashTbl(*objBinHashTablesParamsCreation.sNodeHshTbl)
        WayHshTbl      = BinHddHashTbl(*objBinHashTablesParamsCreation.sWayHshTbl)
        RelationHshTbl = BinHddHashTbl(*objBinHashTablesParamsCreation.sRelationHshTbl)

        ConvertTypes      = lambda x: (long(x[0]), float(x[2]), float(x[1]))
        nCurrentOffset    = 0
        nRelationWaysCnt  = 0
        isFirstWayForRead = True
        isFirstRelForRead = True

        # Start fill nodes
        for sBlock in ReadFileByBlock(sOsmPath, nBlockSize):
            NodeHshTbl.SetListValues(map(ConvertTypes, re.findall(NodeFullPattern, sBlock)))
            sBlock = sBlock.encode('utf-8')
            nCurrentOffset += len(sBlock)

            # Start fill ways
            if sWayPatternForGetCount in sBlock:
                isFirstWayForRead = ParseOsmBlockIntoBinHashTable(WayHshTbl, sWayPatternForGetCount, sBlock, nCurrentOffset, WayFullPattern, isFirstWayForRead)

            # Start fill relations
            if sRelationPatternForGetCount in sBlock:
                isFirstRelForRead = ParseOsmBlockIntoBinHashTable(RelationHshTbl, sRelationPatternForGetCount, sBlock, nCurrentOffset, RelFullPattern, isFirstRelForRead)

            if isFirstRelForRead == False:
                MemberSearchResult  = re.findall(RelMemberFullPattern, sBlock)
                nRelationWaysCnt += len(MemberSearchResult)

        nNodeCnt = NodeHshTbl.TotalValuesCnt
        sMessage = 'Filled Node count: %s'     % nNodeCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        nWayCnt = WayHshTbl.TotalValuesCnt
        sMessage = 'Filled Way count: %s'      % nWayCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        nRelationCnt = RelationHshTbl.TotalValuesCnt
        sMessage = 'Filled Relation count: %s' % nRelationCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMessage = 'Outer/Inner Ways in relations count: %s' % nRelationWaysCnt
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        del NodeHshTbl
        del WayHshTbl
        del RelationHshTbl

        objBinHashTablesParamsCreation.sRelationWaysHshTbl = objBinHashTablesParamsCreation.sRelationWaysHshTbl + (nRelationWaysCnt,)
        RelationWaysHshTbl = HddHashTbl(*objBinHashTablesParamsCreation.sRelationWaysHshTbl)
        RelationWaysHshTbl.CreateHshTblFile()
        RelationWaysHshTbl.FreeHshTbl()

    except:
        sMessage = 'Error in FillHashTables'
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

#-------------------------------------------------------------------------------

def CompressResultShapes(sInputDir = '', sOutputDir = sOutCompressed):

    if sInputDir == '':
        sInputDir = sOutputWorkspace

    # Search shape layers
    listFilePaths = SearchAllFiles(sInputDir, '*.shp')

    # Union paths for same layer name
    setFilePaths         = set()
    listExceptLayers = []

    for sShapeFilePath in listFilePaths:
        # Search file counter in shape name
        sShapeFileName      = os.path.basename(sShapeFilePath)
        sSourceShapeCounter = sShapeFileName.split('.')[0].split('_')[-1]
        nSourceShapeCounter = int(sSourceShapeCounter) if sSourceShapeCounter.isdigit() else 0

        # Replace file counter on '*'
        if nSourceShapeCounter <> 0:

            sShapeFileName               = sShapeFileName.replace('_%s' % sSourceShapeCounter, '*')
            sFirstShapeFileNameForExcept = sShapeFileName.replace('*', '')
            listExceptLayers.append(os.path.join(os.path.dirname(sShapeFilePath), sFirstShapeFileNameForExcept))
            sShapeFilePath = os.path.join(os.path.dirname(sShapeFilePath), sShapeFileName)

        setFilePaths.add(sShapeFilePath)

    # Exclude first shapes for layers with parts
    for sShapeFielForExlude in listExceptLayers:
        setFilePaths.discard(sShapeFielForExlude)

    # Compress all shape layers into result
    for sShapeFilePath in setFilePaths:

           sLayerName = os.path.basename(sShapeFilePath).split('.')[0]
           sShapeFileMssk = '%s.*' % sLayerName
           sZipFileMaskPath = os.path.join(os.path.dirname(sShapeFilePath), sShapeFileMssk)
           sOutputZipPath = os.path.join(sOutputDir, '%s.zip' % sLayerName.replace('*', ''))
           ZipArchive(sZipFileMaskPath, sOutputZipPath)

#-------------------------------------------------------------------------------

def FiltrationNodesIntoPointLayers(sOsmPath, dictConditionIndex, dictLayersForInsert, dictLayerParams):
    try:

        OsmFile          = open(sOsmPath, 'rb')
        sFeatureType     = objGeomFeatureTypes.sPoint
        sOsmFeatureType  = objOsmMainTags.sNode
        OsmFileGenerator = (sCurrReadLine.decode("utf-8") for sCurrReadLine in OsmFile.xreadlines())
        nNodeCounter     = 0
        timeStartTime    = datetime.datetime.today()
        sTagForSearch    = '<%s' % objOsmMainTags.sNode
        sWayTagFinRead   = '<%s' % objOsmMainTags.sWay
        sRelTagFinRead   = '<%s' % objOsmMainTags.sRelation
        sCurrCloseFeatureTag = '</%s>' % sOsmFeatureType

        for CurrLayerName in dictLayersForInsert[sFeatureType]:
            dictLayersForInsert[sFeatureType][CurrLayerName][0] = ogr.Open(dictLayersForInsert[sFeatureType][CurrLayerName][0], update = 1)

        for sCurrReadLine in OsmFileGenerator:
            # Check node in current string
            if sCurrReadLine.count(sTagForSearch) > 0:
                # Match node with attribute
                NodeFindResult = re.search(AttributeNodePattern, sCurrReadLine)

                if NodeFindResult and len(NodeFindResult.groups()) > 0:
                    # Get Node Id
                    nNodeId = float(NodeFindResult.groups()[0])

                    # Copy header line
                    sNodeHeaderLine = sCurrReadLine

                    sCurrReadLine = OsmFileGenerator.next()

                    setLayersForInsert, dictAttributes, nCurrSeek = ReadAttributes(sCurrReadLine, OsmFileGenerator, sOsmFeatureType, nNodeId, dictConditionIndex, sFeatureType, sCurrCloseFeatureTag)

                    if len(setLayersForInsert) > 0:
                        for sLayersForInsert in setLayersForInsert:
                            # Get Fclass field name for current layer
                            sCurrFclassTag   = dictLayersForInsert[sFeatureType][sLayersForInsert][2]
                            # Get fclass value if exist and field list for indert value
                            listFields       = dictLayersForInsert[sFeatureType][sLayersForInsert][1]

                            # Get Point coordinates
                            tplCoordinates = ParseCoordinates(sNodeHeaderLine)

                            try:

                                # Create geometry
                                geomCurrPoint = ogr.Geometry(ogr.wkbPoint)
                                geomCurrPoint.SetPoint(0,*tplCoordinates)

                                # Set attributes into object (with field mapping)
                                geomCurrFeature = SetAttributeToFeature(geomCurrPoint, dictLayersForInsert, listFields, dictAttributes, sFeatureType, sLayersForInsert,  objOsmMainIds.sNodeId, nNodeId)

                                dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).CreateFeature(geomCurrFeature)

                                PrintGdalError()

                                dictLayersForInsert = SplitShapeFiles(dictLayersForInsert, sFeatureType, sLayersForInsert, geomCurrFeature)

                            except:
                                sMessage = 'Error with create %s geometry for ID = %d, layer %s' % (sFeatureType, nNodeId, sLayersForInsert)
                                PrintMessageToLog(objLogPaths.sCreatePntGeomErrorsLogPath, sMessage)

                            # Print each 100 000 inserted point stat
                            nNodeCounter += 1

                            if nNodeCounter % 100000 == 0:
                                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nNodeCounter, datetime.datetime.today() - timeStartTime)
                                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                                timeStartTime = datetime.datetime.today()

            # Break read osm file if found way or relation line
            elif sCurrReadLine.count(sWayTagFinRead) > 0 or sCurrReadLine.count(sRelTagFinRead) > 0:
                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nNodeCounter, datetime.datetime.today() - timeStartTime)
                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                break

        OsmFile.close()

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

#-------------------------------------------------------------------------------
def FiltrationWaysIntoPolylineLayers(sOsmPath, WayHashTbl, NodeHashTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams):
    try:

        nWaySeek = WayHashTbl.nOffsetForRead
        #OsmFileGenerator = OsmReaderFileGenerator(sOsmPath, nWaySeek)
        #OsmFile  = open(sOsmPath, 'rb', )

        OsmFile = open(sOsmPath, "rb")
        OsmFile.seek(nWaySeek)
        OsmFileGenerator = (sCurrReadLine.decode("utf-8") for sCurrReadLine in OsmFile.xreadlines())
        #OsmFileGenerator = OsmFileReaderGen(OsmFile)
        sFeatureType     = objGeomFeatureTypes.sPolyLine
        sOsmFeatureType  = objOsmMainTags.sWay
        nWayCounter      = 1
        timeStartTime    = datetime.datetime.today()
        sTagForSearch    = '<%s' % objOsmMainTags.sWay
        sRelTagFinRead   = '<%s' % objOsmMainTags.sRelation
        sCurrCloseFeatureTag = '</%s>' % sOsmFeatureType

        for CurrLayerName in dictLayersForInsert[sFeatureType]:
            dictLayersForInsert[sFeatureType][CurrLayerName][0] = ogr.Open(dictLayersForInsert[sFeatureType][CurrLayerName][0], update = 1)

        for sCurrReadLine in OsmFileGenerator:

            # Search way tag
            if sTagForSearch in sCurrReadLine:

                WayFindResult = re.search(WayPattern, sCurrReadLine)
                # Get Way Id
                if WayFindResult and len(WayFindResult.groups()) > 0:

                    nWayId = float(WayFindResult.groups()[0])

                    # Get Nodes IDs
                    listNodesIds  = []

                    for sCurrReadLine in OsmFileGenerator:

                        if sNdRefPattern in sCurrReadLine > 0:

                            NdLinkSearchResult = re.search(NdRefPattern, sCurrReadLine)

                            if NdLinkSearchResult and len(NdLinkSearchResult.groups()) > 0:
                                nNodeId = long(NdLinkSearchResult.groups()[0])
                                listNodesIds.append(nNodeId)

                        else:
                            break

                    # Simple check way geometries
                    if len(listNodesIds) < 2:
                        continue

                    # Get Way attributes
                    setLayersForInsert, dictAttributes, nWaySeek = ReadAttributes(sCurrReadLine, OsmFileGenerator, sOsmFeatureType, nWayId, dictConditionIndex, sFeatureType, sCurrCloseFeatureTag, nWaySeek)

                    if len(setLayersForInsert) > 0:
                        # Get line geometry

                        listCoordinates  = GetWayCoordinates(NodeHashTbl, listNodesIds, nWayId)

                        if len(listCoordinates) < 2:
                            sMessage = 'In %s %s wrong geometries' % (objOsmMainIds.sWayId, nWayId)
                            PrintMessageToLog(objLogPaths.sCreateLnGeomErrorsLogPath, sMessage)
                            continue

                        for sLayersForInsert in setLayersForInsert:

                            # Get Fclass field name for current layer
                            #sCurrFclassTag   = dictLayersForInsert[sFeatureType][sLayersForInsert][2]

                            # Get fclass value if exist and field list for indert value
                            #sFclassValue     = dictAttributes.get(sCurrFclassTag, '')
                            listFields       = dictLayersForInsert[sFeatureType][sLayersForInsert][1]

                            try:
                                # Get Polyline
                                geomCurrPolyLine = ogr.Geometry(ogr.wkbLineString)
                                for tplCoordinates in listCoordinates:
                                   geomCurrPolyLine.AddPoint(*tplCoordinates)

                                # Set attributes into object (with field mapping)
                                geomCurrFeature = SetAttributeToFeature(geomCurrPolyLine, dictLayersForInsert, listFields, dictAttributes, sFeatureType, sLayersForInsert,  objOsmMainIds.sWayId, nWayId)

                                dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).CreateFeature(geomCurrFeature)

                                PrintGdalError()

                                # Split shapefiles than more 2 Gb
                                dictLayersForInsert = SplitShapeFiles(dictLayersForInsert, sFeatureType, sLayersForInsert, geomCurrFeature)

                            except:
                                sMessage = 'Error with create %s geometry for ID = %d, layer %s' % (sFeatureType, nWayId, sLayersForInsert)
                                PrintMessageToLog(objLogPaths.sCreateLnGeomErrorsLogPath, sMessage)

                                sMessage = 'Point list : %s' % (str(listCoordinates))
                                PrintMessageToLog(objLogPaths.sCreateLnGeomErrorsLogPath, sMessage)

                                break

                            # Print each 100 000 inserted point stat
                            nWayCounter += 1

                            if nWayCounter % 100000 == 0:
                                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nWayCounter, datetime.datetime.today() - timeStartTime)
                                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                                timeStartTime = datetime.datetime.today()

            elif sRelTagFinRead in sCurrReadLine > 0:
                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nWayCounter, datetime.datetime.today() - timeStartTime)
                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                break

        OsmFile.close()

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

#-------------------------------------------------------------------------------

def FiltrationWaysIntoPolygonLayers(sOsmPath, WayHashTbl, NodeHashTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams, RelationWaysHshTbl):
    try:

        OsmFile  = open(sOsmPath, 'rb')
        nWaySeek = WayHashTbl.nOffsetForRead
        OsmFile.seek(nWaySeek)

        sFeatureType     = objGeomFeatureTypes.sPolygon
        sOsmFeatureType  = objOsmMainTags.sWay
        OsmFileGenerator = (sCurrReadLine.decode("utf-8") for sCurrReadLine in OsmFile.xreadlines())
        nWayCounter      = 1
        timeStartTime    = datetime.datetime.today()
        sTagForSearch    = '<%s' % objOsmMainTags.sWay
        sRelTagFinRead   = '<%s' % objOsmMainTags.sRelation
        sWayCloseTagFinRead = '</%s' % objOsmMainTags.sWay

        ##for CurrLayerName in dictLayersForInsert[sFeatureType]:
        ##    dictLayersForInsert[sFeatureType][CurrLayerName][0] = ogr.Open(dictLayersForInsert[sFeatureType][CurrLayerName][0], update = 1)

        for sCurrReadLine in OsmFileGenerator:
            nWaySeek += len(sCurrReadLine)
            # Search way tag
            if sCurrReadLine.count(sTagForSearch) > 0:

                WayFindResult = re.search(WayPattern, sCurrReadLine)
                # Get Way Id
                if WayFindResult and len(WayFindResult.groups()) > 0:

                    nWayId = long(WayFindResult.groups()[0])

                    # Get Way IDs
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

                    if (sCurrReadLine.count(sWayCloseTagFinRead) > 0
                       or len(listNodesIds) < 3                #check polygon conditions
                       or listNodesIds[0] <> listNodesIds[-1]):
                        continue

                    # Get Way attributes
                    setLayersForInsert, dictAttributes, nWaySeek = ReadAttributes(sCurrReadLine, OsmFileGenerator, sOsmFeatureType, nWayId, dictConditionIndex, sFeatureType, sWayCloseTagFinRead, nWaySeek)

                    if len(setLayersForInsert) > 0:

                        setLayersForInsertFiltered = copy.deepcopy(setLayersForInsert)

                        for sLayersForInsert in setLayersForInsert:
                            if RelationWaysHshTbl[nWayId]:
                               setLayersForInsert = set()
                               break

                    if len(setLayersForInsert) > 0:
                        # Get line geometry
                        listCoordinates  = GetWayCoordinates(NodeHashTbl, listNodesIds, nWayId)

                        if len(listCoordinates) < 3 or listCoordinates[0] <> listCoordinates[-1]:
                            sMessage = 'In way %s wrong geometries for polygon' % (nWayId)
                            PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)
                            continue

                        for sLayersForInsert in setLayersForInsert:

                            # Get Fclass field name for current layer
                            sCurrFclassTag   = dictLayersForInsert[sFeatureType][sLayersForInsert][2]

                            # Get fclass value if exist and field list for indert value
                            sFclassValue     = dictAttributes.get(sCurrFclassTag, '')
                            listFields       = dictLayersForInsert[sFeatureType][sLayersForInsert][1]

                            try:
                                # Get Polyline

                                geomCurrentClosedLine = ogr.Geometry(ogr.wkbLinearRing)
                                for tplCoordinates in listCoordinates:
                                   geomCurrentClosedLine.AddPoint(*tplCoordinates)

                                geomCurrentPolygon = ogr.Geometry(ogr.wkbPolygon)
                                geomCurrentPolygon.AddGeometry(geomCurrentClosedLine)

                                # Set attributes into object (with field mapping)
                                geomCurrFeature = SetAttributeToFeature(geomCurrentPolygon, dictLayersForInsert, listFields, dictAttributes, sFeatureType, sLayersForInsert,  objOsmMainIds.sWayId, nWayId)

                                dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).CreateFeature(geomCurrFeature)

                                PrintGdalError()

                                # Split shapefiles than more 2 Gb
                                dictLayersForInsert = SplitShapeFiles(dictLayersForInsert, sFeatureType, sLayersForInsert, geomCurrFeature)

                            except:
                                sMessage = 'Error with create %s geometry for %s = %d, layer %s' % (sFeatureType, objOsmMainIds.sWayId, nWayId, sLayersForInsert)
                                PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

                                sMessage = 'Point list : %s' % (str(listCoordinates))
                                PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

                                break

                            # Print each 100 000 inserted point stat
                            nWayCounter += 1

                            if nWayCounter % 100000 == 0:
                                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nWayCounter, datetime.datetime.today() - timeStartTime)
                                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                                timeStartTime = datetime.datetime.today()

            elif sCurrReadLine.count(sRelTagFinRead) > 0:
                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nWayCounter, datetime.datetime.today() - timeStartTime)
                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                break

        OsmFile.close()

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

#-------------------------------------------------------------------------------

def FiltrationRelationsIntoPolygonLayers(sOsmPath, RelationHashTbl, WayHashTbl, NodeHashTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams, RelationWaysHshTbl):
    try:

        OsmFile  = open(sOsmPath, 'rb')
        nRelationSeek = RelationHashTbl.nOffsetForRead
        OsmFile.seek(nRelationSeek)

        sFeatureType     = objGeomFeatureTypes.sPolygon
        sOsmFeatureType  = objOsmMainTags.sRelation
        OsmFileGenerator = (sCurrReadLine.decode("utf-8") for sCurrReadLine in OsmFile.xreadlines())
        nRelCounter      = 0
        timeStartTime    = datetime.datetime.today()

        sTagForSearch    = '<%s' % objOsmMainTags.sRelation
        sMemTagFinRead   = '<%s' % objOsmMainTags.sMember
        sRelCloseTagFinRead = '</%s' % objOsmMainTags.sRelation
        sOsmCloseTagFinRead = '</%s' % objOsmMainTags.sOsm

        for CurrLayerName in dictLayersForInsert[sFeatureType]:
            dictLayersForInsert[sFeatureType][CurrLayerName][0] = ogr.Open(dictLayersForInsert[sFeatureType][CurrLayerName][0], update = 1)

        for sCurrReadLine in OsmFileGenerator:
            nRelationSeek += len(sCurrReadLine.encode('utf-8'))
            # Search relation tag
            if sCurrReadLine.count(sTagForSearch) > 0:

                RelationFindResult = re.search(RelPattern, sCurrReadLine)
                # Get relation Id
                if RelationFindResult and len(RelationFindResult.groups()) > 0:

                    nRelationId = float(RelationFindResult.groups()[0])

                    # Get member IDs
                    dictRingWay = {}

                    for sCurrReadLine in OsmFileGenerator:
                        nRelationSeek += len(sCurrReadLine.encode('utf-8'))
                        if sCurrReadLine.count(sMemTagFinRead) > 0:

                            MemberSearchResult = re.search(RelMemberPattern, sCurrReadLine)

                            if MemberSearchResult and len(MemberSearchResult.groups()) > 0:
                                sMemberType = MemberSearchResult.groups()[0]
                                sMemberRef  = int(MemberSearchResult.groups()[1])
                                sMemberRole = MemberSearchResult.groups()[2]

                                if sMemberType.lower() == objOsmMainTags.sWay and sMemberRole.upper() in (objOsmPolygonRole.sOuter, objOsmPolygonRole.sInner):
                                   dictRingWay.setdefault(sMemberRole.upper(), []).append(sMemberRef)
                                   RelationWaysHshTbl[sMemberRef] = (0,)
                        else:
                            break

                    if (len(dictRingWay) == 0 or sCurrReadLine.count(sRelCloseTagFinRead) > 0):
                        continue

                    # Get Relation attributes
                    setLayersForInsert, dictAttributes, nRelationSeek = ReadAttributes(sCurrReadLine, OsmFileGenerator, sOsmFeatureType, nRelationId, dictConditionIndex, sFeatureType, sRelCloseTagFinRead, nRelationSeek)

                    if len(setLayersForInsert) > 0:

                        # Get Ways coordinates
                        for sPolygonRingType in dictRingWay:
                            dictRingWay[sPolygonRingType] = GetNodeListFromWay(OsmFile, WayHashTbl, NodeHashTbl, dictRingWay[sPolygonRingType], nRelationSeek, nRelationId)

                        # Get Myltipolygon Geometry
                        MyltipolygonGeometry = BuildMultipolygon(dictRingWay)

                        if not MyltipolygonGeometry:
                            sMessage = 'In %s %s wrong geometries for polygon' % (objOsmMainIds.sRelationId, nRelationId)
                            PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)
                            continue

                        gdal.ErrorReset()

                        if not MyltipolygonGeometry.IsValid():

                            sErrorText  = 'ERROR'
                            sException  = 'ILLEGALARGUMENTEXCEPTION'

                            PrintGdalError()

                            if ( sErrorText in gdal.GetLastErrorMsg().upper() or
                                 sException in gdal.GetLastErrorMsg().upper()):

                                sMessage = 'In %s %s invalid polygon geometry' % (objOsmMainIds.sRelationId, nRelationId)
                                PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

                                continue

                        gdal.ErrorReset()

                        for sLayersForInsert in setLayersForInsert:
                            # Get Fclass field name for current layer
                            sCurrFclassTag   = dictLayersForInsert[sFeatureType][sLayersForInsert][2]

                            # Get fclass value if exist and field list for indert value
                            sFclassValue     = dictAttributes.get(sCurrFclassTag, '')
                            listFields       = dictLayersForInsert[sFeatureType][sLayersForInsert][1]

                            try:
                                # Insert Polygon

                                # Set attributes into object (with field mapping)
                                geomCurrFeature = SetAttributeToFeature(MyltipolygonGeometry, dictLayersForInsert, listFields, dictAttributes, sFeatureType, sLayersForInsert,  objOsmMainIds.sRelationId, nRelationId)

                                dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetLayer(0).CreateFeature(geomCurrFeature)

                                # Split shapefiles than more 2 Gb
                                dictLayersForInsert = SplitShapeFiles(dictLayersForInsert, sFeatureType, sLayersForInsert, geomCurrFeature)

                                '''
                                # Uncomment if boundary ply dont build
                                if nRelCounter % 10000 == 0:
                                    sSourceShapePath = dictLayersForInsert[sFeatureType][sLayersForInsert][0].GetName()
                                    dictLayersForInsert[sFeatureType][sLayersForInsert][0].Destroy()
                                    dictLayersForInsert[sFeatureType][sLayersForInsert][0] = ogr.Open(sSourceShapePath, update = 1)
                                '''

                            except:
                                sMessage = 'Error with create %s geometry for %s = %d, layer %s' % (sFeatureType, objOsmMainIds.sRelationId, nRelationId, sLayersForInsert)
                                PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

                                sMessage = 'Point list : %s' % (str(dictRingWay))
                                PrintMessageToLog(objLogPaths.sCreatePolyGeomErrorsLogPath, sMessage)

                                break

                            # Print each 100 000 inserted point stat
                            nRelCounter += 1

                            if nRelCounter % 100000 == 0:
                                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nRelCounter, datetime.datetime.today() - timeStartTime)
                                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                                timeStartTime = datetime.datetime.today()

            elif sCurrReadLine.count(sOsmCloseTagFinRead) > 0:
                sMessage = '\t%s inserted %s %s' % (sOsmFeatureType, nRelCounter, datetime.datetime.today() - timeStartTime)
                PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)
                break

        OsmFile.close()

        return RelationWaysHshTbl
    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit
#-------------------------------------------------------------------------------

def SearchShapefilesAndBuildSpatialIndex():
    try:

        sMessage = 'Building spatial indexes for all shapes in result folder'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMask = '*.shp'
        listShapeFiles = SearchAllFiles(sOutputWorkspace, sMask)

        for sShapeFilePath in listShapeFiles:
            sShapeFileName = os.path.basename(sShapeFilePath)
            sMessage       = '\t%s' % sShapeFileName
            PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

            BuildingQgisSpatialIndex(sShapeFilePath)

        sMessage       = 'Finish building spatial indexes'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

#-------------------------------------------------------------------------------
def CreateOutputFeatures():

    dictLayerParams    = ReadXMLParemeters(sXMLFilePath)
    dictConditionIndex = {}

    sMessage = '-'*30
    PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

    sMessage = 'Create output layers'
    PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

    dictLayersForInsert = {}

    for sFeatureType in dictLayerParams:
        sFeatureType = sFeatureType.upper()
        for listCurrLayerParam in dictLayerParams[sFeatureType]:
            sLayerName      = listCurrLayerParam[0]
            listLayerFields = listCurrLayerParam[1]
            listConditions  = listCurrLayerParam[2]

            for sCurrentCondition in listConditions:
                sCurrStrCondition = u'%s_%s' % (sCurrentCondition[0], sCurrentCondition[1])
                dictConditionIndex.setdefault(sFeatureType, {}).setdefault(sCurrStrCondition.upper(),[]).append(sLayerName)

            CurrLayerPath   = os.path.join(sOutputWorkspace, '%s.shp' % sLayerName)
            sMainTagName    = listConditions[0][0]
            curShapeLayer, listLayerFieldsMapping = CreateFeature(CurrLayerPath, sFeatureType, listLayerFields, sMainTagName, isInsertMainTag)
            dictLayersForInsert.setdefault(sFeatureType, {})[sLayerName] = [curShapeLayer, listLayerFieldsMapping, sMainTagName]

    if objGeomFeatureTypes.sPoint in dictLayersForInsert:

        sMessage = 'Fill point feature classes'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        FiltrationNodesIntoPointLayers(sOsmPath, dictConditionIndex, dictLayersForInsert, dictLayerParams)

        for CurrLayerName in dictLayersForInsert[objGeomFeatureTypes.sPoint]:
            dictLayersForInsert[objGeomFeatureTypes.sPoint][CurrLayerName][0].Destroy()

    if objGeomFeatureTypes.sPolyLine in dictLayersForInsert:

        sMessage = 'Fill polyline feature classes'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        NodeHshTbl     = BinHddHashTbl(*objBinHashTablesParamsOpen.sNodeHshTbl)
        NodeHshTbl.CreateIndex()

        WayHshTbl      = BinHddHashTbl(*objBinHashTablesParamsOpen.sWayHshTbl)

        FiltrationWaysIntoPolylineLayers(sOsmPath, WayHshTbl, NodeHshTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams)

        for CurrLayerName in dictLayersForInsert[objGeomFeatureTypes.sPolyLine]:
            dictLayersForInsert[objGeomFeatureTypes.sPolyLine][CurrLayerName][0].Destroy()

    if objGeomFeatureTypes.sPolygon in dictLayersForInsert:

        sMessage = 'Fill polygons feature classes from Relations'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        if 'NodeHshTbl' not in locals():
            NodeHshTbl     = BinHddHashTbl(*objBinHashTablesParamsOpen.sNodeHshTbl)
            NodeHshTbl.CreateIndex()

        if 'WayHshTbl' not in locals():
            WayHshTbl      = BinHddHashTbl(*objBinHashTablesParamsOpen.sWayHshTbl)

        WayHshTbl.CreateIndex()

        RelationHshTbl     = BinHddHashTbl(*objBinHashTablesParamsOpen.sRelationHshTbl)
        RelationHshTbl.CreateIndex()

        # Get nRelationWaysCnt
        RelationWaysHshTbl = HddHashTbl(*objBinHashTablesParamsOpen.sRelationWaysHshTbl)
        nRelationWaysCnt = RelationWaysHshTbl.GetHeader()[1]
        RelationWaysHshTbl.FreeHshTbl()

        # Recreate RelationWaysHshTbl
        objBinHashTablesParamsCreation.sRelationWaysHshTbl = objBinHashTablesParamsCreation.sRelationWaysHshTbl + (nRelationWaysCnt,)
        RelationWaysHshTbl = HddHashTbl(*objBinHashTablesParamsCreation.sRelationWaysHshTbl)
        RelationWaysHshTbl.CreateHshTblFile()

        RelationWaysHshTbl = FiltrationRelationsIntoPolygonLayers(sOsmPath, RelationHshTbl, WayHshTbl, NodeHshTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams, RelationWaysHshTbl)

        sMessage = 'Fill polygons feature classes from Ways'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        FiltrationWaysIntoPolygonLayers(sOsmPath, WayHshTbl, NodeHshTbl, dictConditionIndex, dictLayersForInsert, dictLayerParams, RelationWaysHshTbl)

        for CurrLayerName in dictLayersForInsert[objGeomFeatureTypes.sPolygon]:
            dictLayersForInsert[objGeomFeatureTypes.sPolygon][CurrLayerName][0].Destroy()

#@profile
def main():
    if len(sys.argv) > 1:

        global sOsmPath, sXMLFilePath, sOperation, sOutputWorkspace

        sOsmPath     = sys.argv[1]
        sXMLFilePath = sys.argv[2]
        sOperation   = sys.argv[3]

        if len(sys.argv) > 4:
           sOutputWorkspace = sys.argv[4]

        dictOperationMapping = {'FillBinHashTables'                    : FillBinHashTables,
                                'CreateOutputFeatures'                 : CreateOutputFeatures,
                                'SearchShapefilesAndBuildSpatialIndex' : SearchShapefilesAndBuildSpatialIndex,
                                'CompressResultShapes'                 : CompressResultShapes}

        objOperation  = dictOperationMapping.get(sOperation, None)

        timeStartTime = datetime.datetime.today()

        if objOperation:
            sMessage = 'Start %s' % sOperation
            PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

            objOperation()
        else:
            sMessage = 'No jobs!'
            PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sMessage = 'Finish \t%s' % (datetime.datetime.today() - timeStartTime)
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

if __name__ == '__main__':
    main()

