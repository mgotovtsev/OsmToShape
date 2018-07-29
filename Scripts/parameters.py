import os
import sys
import re
import codecs

sScriptsDir    = os.path.dirname(sys.argv[0])
sRootDir       = os.path.dirname(sScriptsDir)

sSourceDir       = os.path.join(sRootDir, '01_Source')
sScriptsDir      = os.path.join(sRootDir, '02_Scripts')
sOutputWorkspace = os.path.join(sRootDir, '03_Results')
sTempDir         = r'c:\_USERS\Mgotovtsev\OSM\Temp' #os.path.join(sRootDir, '04_Temp')
sBackupsDir      = os.path.join(sRootDir, '05_Backups')
sTestingDir      = os.path.join(sRootDir, '06_Testing')
sDocumentsDir    = os.path.join(sRootDir, '07_Documents')
sLogsDir         = os.path.join(sRootDir, '08_Logs')
sUtilitsDir      = os.path.join(sRootDir, '09_Utilits')
sConfDir         = os.path.join(sScriptsDir, 'conf')
sOutCompressed   = os.path.join(sOutputWorkspace, 'Compressed')

sOsmFilterToolPath = os.path.join(sUtilitsDir, r'osmfilter\osmfilter.exe')
s7zipUtilityPath   = os.path.join(sUtilitsDir, r'7z\7za.exe')

#sOsmFilePath       = 'andorra-latest.osm'
#sOsmFilePath       = 'UnatedStatesBndy.osm'
#sOsmFilePath       = 'delaware-latest.osm'

sOsmFileName       = 'North and South Americas.osm'
sConfogXmlFileName = 'sample_osm.xml'

sGenerator         = ''#"JOSM"

sOsmPath     = os.path.join(sSourceDir, sOsmFileName)
sXMLFilePath = os.path.join(sConfDir, sConfogXmlFileName)

if sGenerator == "JOSM":
    NodePattern                 = re.compile(r"<node id='(\d+)'(.*?)>")
    WayPattern                  = re.compile(r"<way id='(\d+)'(.*?)>")
    RelPattern                  = re.compile(r"<relation id='(\d+)'(.*?)>")
    NodeFullPattern             = re.compile(r"<node id='(\d+)'.+lat='(-?\d+.\d*)' lon='(-?\d+.\d*)'")
    WayFullPattern              = re.compile(r"<way id='(\d+)'")
    RelFullPattern              = re.compile(r"<relation id='(\d+)'")
    SimpleNodePattern           = re.compile(r"<node id='(\d+)'(.*?)' />")
    AttributeNodePattern        = re.compile(r"<node id='(\d+)'(.*?)'>")
    KeyValuePattern             = re.compile(r"<tag k='(.*?)' v='(.*?)' />")
    LatPattern                  = re.compile(r"lat='(-?\d+.\d*)'")
    LonPattern                  = re.compile(r"lon='(-?\d+.\d*)'")
    NdRefPattern                = re.compile(r"<nd ref='(\d+)' />")
    RelMemberPattern            = re.compile(r"<member type='(.*?)' ref='(\d+)' role='(.*?)' />")
    RelMemberFullPattern        = re.compile(r"<member type='(way|WAY)' ref='(\d+)' role='(outer|inner|OUTER|INNER)'")
    sNdRefPattern               = "<nd ref='"
    sNodePatternForGetCount     = "<node id='"
    sWayPatternForGetCount      = "<way id='"
    sRelationPatternForGetCount = "<relation id='"
else:
    NodePattern                 = re.compile(r'<node id="(\d+)"(.*?)>')
    WayPattern                  = re.compile(r'<way id="(\d+)"(.*?)>')
    RelPattern                  = re.compile(r'<relation id="(\d+)"(.*?)>')
    NodeFullPattern             = re.compile(r'<node id="(\d+)".+lat="(-?\d+.\d*)" lon="(-?\d+.\d*)"')
    WayFullPattern              = re.compile(r'<way id="(\d+)"')
    RelFullPattern              = re.compile(r'<relation id="(\d+)"')
    SimpleNodePattern           = re.compile(r'<node id="(\d+)"(.*?)"/>')
    AttributeNodePattern        = re.compile(r'<node id="(\d+)"(.*?)">')
    KeyValuePattern             = re.compile(r'<tag k="(.*?)" v="(.*?)"/>')
    LatPattern                  = re.compile(r'lat="(-?\d+.\d*)"')
    LonPattern                  = re.compile(r'lon="(-?\d+.\d*)"')
    NdRefPattern                = re.compile(r'<nd ref="(\d+)"/>')
    RelMemberPattern            = re.compile(r'<member type="(.*?)" ref="(\d+)" role="(.*?)"/>')
    RelMemberFullPattern        = re.compile(r'<member type="(way|WAY)" ref="(\d+)" role="(outer|inner|OUTER|INNER)"')
    sNdRefPattern               = '<nd ref="'
    sNodePatternForGetCount     = '<node id="'
    sWayPatternForGetCount      = '<way id="'
    sRelationPatternForGetCount = '<relation id="'

# Block size for read OSM file when bin hash tables will filled
nBlockSize = 10 * (1024 ** 2) # (1024 ** 2) - MegaByte

sFclassFieldName = 'main_tag'
isInsertMainTag  = False

class GeomFeatureTypes():
    def __init__(self):
        self.sPoint    = 'POINT'
        self.sPolyLine = 'POLYLINE'
        self.sPolygon  = 'POLYGON'

class OsmMainTags():
    def __init__(self):
        self.sNode     = 'node'
        self.sWay      = 'way'
        self.sRelation = 'relation'
        self.sMember   = 'member'
        self.sOsm      = 'osm'

class OsmMainIds():
    def __init__(self):
        self.sNodeId     = 'NodeId'
        self.sWayId      = 'WayId'
        self.sRelationId = 'RelationId'

class OsmPolygonRole():
    def __init__(self):
        self.sOuter = 'OUTER'
        self.sInner = 'INNER'

class HashTablesParameters():
    def __init__(self):
        self.sNodeHshTbl     = ('NodeHshTbl',-1,'qdd')
        self.sWayHshTbl      = ('WayHshTbl',)
        self.sRelationHshTbl = ('RelationHshTbl',)
        self.sRelationWaysHshTbl = ('RelationWaysHshTbl',)

class BinHashTablesParams():
    def __init__(self, isCreation):
        self.sNodeHshTbl         = ('NodeBinHshTbl.bhsh',     'qdd', isCreation,)
        self.sWayHshTbl          = ('WayBinHshTbl.bhsh',      'qq',  isCreation,)
        self.sRelationHshTbl     = ('RelationBinHshTbl.bhsh', 'qq',  isCreation,)
        self.sRelationWaysHshTbl = ('RelationWaysHshTbl.hsh', 'qq',)

class LogPaths():
    def __init__(self):
        self.sConvertProcessLogPath       = os.path.join(sLogsDir, "ConvertProcessLog.log")
        self.sCommonProcessErrorsLogPath  = os.path.join(sLogsDir, "CommonProcessErrorsLog.log")
        self.sAttributesErrorsLogPath     = os.path.join(sLogsDir, "AttributesErrorsLog.log")
        self.sCreatePntGeomErrorsLogPath  = os.path.join(sLogsDir, "CreatePntGeomErrorsLog.log")
        self.sCreateLnGeomErrorsLogPath   = os.path.join(sLogsDir, "CreateLnGeomErrorsLog.log")
        self.sCreatePolyGeomErrorsLogPath = os.path.join(sLogsDir, "CreatePolyGeomErrorsLog.log")
        self.sGdalErrorsLogPath           = os.path.join(sLogsDir, "GdalErrorsLog.log")

objGeomFeatureTypes            = GeomFeatureTypes()
objOsmMainTags                 = OsmMainTags()
objOsmMainIds                  = OsmMainIds()
objOsmPolygonRole              = OsmPolygonRole()
objHashTablesParameters        = HashTablesParameters()
objBinHashTablesParamsCreation = BinHashTablesParams(True)
objBinHashTablesParamsOpen     = BinHashTablesParams(False)
objLogPaths                    = LogPaths()

dictHtmlSpecSymbolsMapping = {'&#39;' : '\'',
                              '&#38;' : '&',
                              '&#34;' : '"'}