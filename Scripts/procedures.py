#BUILTIN------------------------------------------------------------------------
import os
import sys
import time
import fnmatch
import traceback
import subprocess
#GDAL---------------------------------------------------------------------------
import gdal
import ogr
import osr
import gdalnumeric
import gdalconst
#PROJECT------------------------------------------------------------------------
from model_parameters import *


#-------------------------------------------------------------------

def PrintMessageInFile(sLogFilePath, sMessage):
    try:

        if sLogFilePath == objLogPaths.sConvertProcessLogPath:
           print sMessage

        fileLogFile = open(sLogFilePath, 'a') #exception
        fileLogFile.write(sMessage + "\n")    #exception
    except:
        print traceback.format_exc()
    finally:
        if 'fileLogFile' in locals(): fileLogFile.close() #exception

#-------------------------------------------------------------------

def PrintMessageToLog(sLogFilePath, sMessage = ''):
    sMessage = time.strftime('%d %b %H:%M:%S') + "\tINFO\t" + str(sMessage)
    PrintMessageInFile(sLogFilePath, sMessage)

#-------------------------------------------------------------------

def PrintGdalError():
    if gdal.GetLastErrorMsg() <> '':
        PrintMessageToLog(objLogPaths.sGdalErrorsLogPath, gdal.GetLastErrorMsg())
#-------------------------------------------------------------------

def CopyEmptyShapeFile(sSourceShapePath, sDestShapePath):

    # Define shapefile driver
    DriverName = "ESRI Shapefile"
    ShapeDriver = ogr.GetDriverByName(DriverName)

    # Remove if exist
    if os.path.exists(sDestShapePath):
         ShapeDriver.DeleteDataSource(sDestShapePath)

    # Open source shapefile
    SourceShape = ogr.Open(sSourceShapePath, 0)
    SourceLayer = SourceShape.GetLayerByIndex(0)
    SourceLayerDefn  = SourceLayer.GetLayerDefn()

    # Create output shapefile
    DataSource = ShapeDriver.CreateDataSource(sDestShapePath)
    DestLayer  = DataSource.CreateLayer('SomeLayerName',
                                srs = SourceLayer.GetSpatialRef(),
                                geom_type = SourceLayer.GetLayerDefn().GetGeomType(),
                                options = ['ENCODING=UTF-8'])

    # Add all fields to result shapefile from source
    for i in range(SourceLayerDefn.GetFieldCount()):
        SourceFieldDefn = SourceLayerDefn.GetFieldDefn(i)
        DestLayer.CreateField(SourceFieldDefn)

def BuildMultipolygon(dictRingWay):
    try:
        for sRingType in dictRingWay:

            listCurrCoord         = dictRingWay[sRingType]
            listForComplexPolygon = []
            listForSimplePolygon  = []

            for nIndex in reversed(xrange(0, len(listCurrCoord))):
                listCoordCurrParts = listCurrCoord[nIndex]

                if listCoordCurrParts[0] <> listCoordCurrParts[-1]:
                   listForComplexPolygon.insert(0, listCurrCoord.pop(nIndex))
                else:
                   listForSimplePolygon.insert(0, listCurrCoord.pop(nIndex))

            if len(listForComplexPolygon) > 0:
                listForComplexPolygon = ProcessingBigPolygons(listForComplexPolygon)

            dictRingWay[sRingType] = listForSimplePolygon + listForComplexPolygon

        if dictRingWay.has_key('OUTER') and len(dictRingWay[sRingType]) > 0 :

           ResPolygon = ogr.Geometry(ogr.wkbPolygon)

           for listCoordParts in dictRingWay['OUTER']:
               BorderLine = ogr.Geometry(ogr.wkbLinearRing)
               for tplCoordinates in listCoordParts:
                   BorderLine.AddPoint(*tplCoordinates)

               ResPolygon.AddGeometry(BorderLine)

           listInnerCoordinates = dictRingWay.get('INNER', None)

           if listInnerCoordinates:

               for listCoordParts in dictRingWay['INNER']:
                   BorderLine = ogr.Geometry(ogr.wkbLinearRing)
                   for tplCoordinates in listCoordParts:
                       BorderLine.AddPoint(*tplCoordinates)

                   ResPolygon.AddGeometry(BorderLine)

           return ResPolygon
    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

    return None


def ProcessingBigPolygons(listPolygonParts):
    try:
        listResultParts = []
        nIndex    = 0
        nSubIndex = nIndex + 1
        IsMergeExists = False

        while (nSubIndex < len(listPolygonParts)):

            if listPolygonParts[nIndex][0] == listPolygonParts[nSubIndex][-1]:
               listPolygonParts[nIndex].reverse()

            if listPolygonParts[nIndex][-1] == listPolygonParts[nSubIndex][-1]:
               listPolygonParts[nSubIndex].reverse()

            if listPolygonParts[nIndex][-1] == listPolygonParts[nSubIndex][0]:

               listPolygonParts[nIndex] = listPolygonParts[nIndex] + listPolygonParts.pop(nSubIndex)[1:]
               IsMergeExists = True

               if listPolygonParts[nIndex][0] == listPolygonParts[nIndex][-1]:
                  listResultParts.append(listPolygonParts.pop(nIndex))
                  nSubIndex = nIndex + 1
                  IsMergeExists = False

            else:
               nSubIndex += 1

            if nSubIndex == len(listPolygonParts) and IsMergeExists == False:
               listPolygonParts.pop(0)
               nSubIndex = nIndex + 1
            elif (nSubIndex >= len(listPolygonParts) and IsMergeExists == True and len(listPolygonParts) > 1):
               IsMergeExists = False
               nSubIndex = nIndex + 1

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

    return listResultParts

def BuildSimplePolygon(listCoordinates):
    try:
        BorderLine = ogr.Geometry(ogr.wkbLinearRing)

        for tplCoordinates in listCoordinates:
           BorderLine.AddPoint(*tplCoordinates)

        ResPolygon = ogr.Geometry(ogr.wkbPolygon)
        ResPolygon.AddGeometry(BorderLine)
    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())

    return ResPolygon

def SearchAllFiles(sDataPath, sMask):
    try:

        listDataPaths = []

        for DirPath, DirNames, Files in os.walk(sDataPath):
            for sCurrFile in Files:
                if fnmatch.fnmatch(sCurrFile, sMask):
                   sCurrFilePath = os.path.join(DirPath, sCurrFile)
                   listDataPaths.append(sCurrFilePath)

    except:
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

    return listDataPaths

def CreateFeature(sOutputFeaturePath, sFeatureType, listFeatureFields, sMainTagName, isInsertMainTag):
    try:

        dictTagToFieldsMapping = {}
        dictFieldsToTagMapping = {}

        # Define output feature location
        sOutputFeatureDir = os.path.dirname(sOutputFeaturePath)
        sOutputFeatureName = os.path.basename(sOutputFeaturePath)

        # Delete exists layer
        for DirPath, DirNames, Files in os.walk(sOutputFeatureDir):
            for sCurrFile in Files:
                sCurrPattern = "%s" % sOutputFeatureName
                if fnmatch.fnmatch(sCurrFile, sCurrPattern):
                   sCurrFilePath = os.path.join(DirPath,sCurrFile)
                   os.remove(sCurrFilePath)

        # Create WGS_1984 Reference object
        spatialReference = osr.SpatialReference()
        spatialReference.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

        PrintGdalError()

        dictFieldTypes = {'POINT'   : ogr.wkbPoint,
                          'POLYLINE': ogr.wkbMultiLineString,
                          'POLYGON' : ogr.wkbPolygon}

        # Create shape file
        ShapeDriver = ogr.GetDriverByName('ESRI Shapefile')
        ShapeData   = ShapeDriver.CreateDataSource(sOutputFeaturePath)
        ShapeLayer  = ShapeData.CreateLayer(sOutputFeatureName, srs = spatialReference, geom_type = dictFieldTypes[sFeatureType], options = ['ENCODING=UTF-8'])

        PrintGdalError()

        dictIdFieldName = {'POINT'   : ['NodeId'],
                           'POLYLINE': ['WayId'],
                           'POLYGON' : ['RelationId', 'WayId']}


        # Add Osm Id field
        for sIdFieldName in dictIdFieldName[sFeatureType]:
            sFieldWidth = 11
            FieldDef = ogr.FieldDefn(sIdFieldName, ogr.OFTString)
            FieldDef.SetWidth(sFieldWidth)
            ShapeLayer.CreateField(FieldDef)

        # Add fclass field
        if isInsertMainTag == True:
            sFieldWidth = 50
            FieldDef = ogr.FieldDefn(sFclassFieldName, ogr.OFTString)
            FieldDef.SetWidth(sFieldWidth)
            ShapeLayer.CreateField(FieldDef)

            dictTagToFieldsMapping.setdefault(sMainTagName, []).append(sFclassFieldName)
            dictFieldsToTagMapping.setdefault(sFclassFieldName, []).append([sMainTagName, 1])

        # Add fields into created layer
        listAddedFields = []
        for sFieldParams in listFeatureFields:

            sTagName            = sFieldParams[0]
            sFieldName          = sFieldParams[1]
            nPriority           = sFieldParams[2]
            sFieldType          = sFieldParams[3]
            nFieldTypeLength    = sFieldParams[4]
            nFieldTypeScale     = sFieldParams[5]
            nFieldTypePrecision = sFieldParams[6]
            sFieldFormula       = sFieldParams[7]

            sFieldName = sFieldName.replace(':', '_')

            dictTagToFieldsMapping.setdefault(sTagName,   []).append(sFieldName)
            dictFieldsToTagMapping.setdefault(sFieldName, []).append([sTagName, nPriority])

            # Not add duplicate fields
            if sFieldName in listAddedFields:
                continue
            else:
                listAddedFields.append(sFieldName)

            # Add field into layer
            sFieldWidth = nFieldTypeLength
            FieldDef    = ogr.FieldDefn(sFieldName, ogr.OFTString)
            FieldDef.SetWidth(sFieldWidth)
            ShapeLayer.CreateField(FieldDef)

            PrintGdalError()

        # Sort by priority field and remove priority int
        for sFieldName in dictFieldsToTagMapping:
            if len(dictFieldsToTagMapping[sFieldName]) <> 1:
               dictFieldsToTagMapping[sFieldName].sort(key=lambda x: x[1])
            dictFieldsToTagMapping[sFieldName] = [sCurrField[0] for sCurrField in dictFieldsToTagMapping[sFieldName]]

        FieldDef.Destroy()
        ShapeData.Destroy()

        return sOutputFeaturePath, [dictTagToFieldsMapping, dictFieldsToTagMapping]

    except:
        sMessage = 'Error create feature for ' + sOutputFeaturePath
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

def BuildingQgisSpatialIndex(sPathToShape):
    try:
        sTableName     = os.path.basename(sPathToShape).split('.')[0]
        sCommandString = 'ogrinfo "%s" -sql "CREATE SPATIAL INDEX ON %s"' % (sPathToShape, sTableName)
        nRectcode      = subprocess.call(sCommandString, shell=True)

        if nRectcode <> 0:
           sMessage = 'Error create index for ' + sPathToShape
           raise SystemExit

    except:
        sMessage = 'Error with create spatial index for ' + sPathToShape
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

def BuildingQgisAttributeIndex(sPathToShape, sFialdName):
    try:
        sTableName     = os.path.basename(sPathToShape).split('.')[0]
        sCommandString = 'ogrinfo "%s" -sql "CREATE INDEX ON %s USING %s"' % (sPathToShape, sTableName, sFialdName)
        nRectcode      = subprocess.call(sCommandString, shell=True)

        if nRectcode <> 0:
           sMessage = 'Error with create attribute index for ' + sPathToShape
           raise SystemExit

    except:
        sMessage = 'Error with create attribute index for ' + sPathToShape
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, traceback.format_exc())
        raise SystemExit

def FilteringOsmData(sInputOsm, sOutputOsm, sFilterParameters = ''):
    """
        Function for filtering osm data
        Args:
            sInputOsm          - OSM file for processing
            sOutputOsm         - Output OSM file
            sFilterParameters  - filtering condition
        Returns:
            -
        Raises:
            1. Exception when some error ocured
    """
    try:

        #<Check input parameters>

        #Check on exists OSM file for converting
        if not os.path.exists(sInputOsm):
           sMessage = '%s not exists' % sInputOsm
           PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
           raise SystemExit

        #</Check input parameters>

        sMessage = 'Starting filtering osm file...'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

        sCmdLine = 'call "%s" "%s" %s -o="%s"' % (sOsmFilterToolPath,
                                               sInputOsm,
                                               sFilterParameters,
                                               sOutputOsm)

        RunCmd = subprocess.Popen(sCmdLine,
                                  shell  = True,
                                  stdout = subprocess.PIPE,
                                  stderr = subprocess.STDOUT)

        sRunCmdOut = RunCmd.stdout.read() #saving tool result

        if sRunCmdOut <> '':
            sMessage = 'Error with filtering osm data!'
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
            raise SystemExit #throw exception

        sMessage = 'Osm file has been filtering...'
        PrintMessageToLog(objLogPaths.sConvertProcessLogPath, sMessage)

    except:
        sMessage = 'Error with FilteringOsmData function!'
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
        raise SystemExit #throw exception

def ZipArchive(sInputPath, sOutputPath):
    try:

        sCommandString = '"%s" a -tzip -aoa "%s" "%s"' % (s7zipUtilityPath, sOutputPath, sInputPath)

        nRectCode = subprocess.call(sCommandString) # Exception

        if nRectCode > 0:
            sMessage = 'Error with ZipArchive %s!' % sInputPath
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sMessage)
            PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sCommandString)

    except:
        sErrorMessage = "Error in function ZipArchive"
        PrintMessageToLog(objLogPaths.sCommonProcessErrorsLogPath, sErrorMessage)
        raise SystemExit # Exception
