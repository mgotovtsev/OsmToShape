REM Step 1. Cut planet pbf by region polygon and store result into osm (xml)

set OsmConvert="OsmToShp\Utilits\osmconvert\osmconvert.exe"
set PbfPath="OsmToShp\Source\PBF\planet-160201.osm.pbf"
set PolyPath="OsmToShp\Templates"
set OsmPath="OsmToShp\OSM_src\Europe and Asia.osm"

echo %date% %time% Start extract Europe and Asia >> osmconvertLog.log
%OsmConvert% %PbfPath% -B=%PolyPath%\Europe and Asia.poly -o=%OsmPath% --drop-author --drop-nodes
echo %date% %time% Finish extract Europe and Asia >> osmconvertLog.log

REM Step 2. Convert osm (xml) into shapefiles

set InterpretatorPath="c:\Python27\python.exe"
set WorkDir="OsmToShp"
set ScriptPath=%WorkDir%\Scripts\convert_osm_to_shape.py

%InterpretatorPath% %ScriptPath% %OsmPath% "" "FillBinHashTables"

set XmlPath=%WorkDir%\Scripts\conf\sample_osm_pnt.xml
%InterpretatorPath% %ScriptPath% %OsmPath% %XmlPath% "CreateOutputFeatures"

set XmlPath=%WorkDir%\Scripts\conf\sample_osm_ln.xml
%InterpretatorPath% %ScriptPath% %OsmPath% %XmlPath% "CreateOutputFeatures"

set XmlPath=%WorkDir%\Scripts\conf\sample_osm_ply.xml
%InterpretatorPath% %ScriptPath% %OsmPath% %XmlPath% "CreateOutputFeatures"

%InterpretatorPath% %ScriptPath% "" "" "SearchShapefilesAndBuildSpatialIndex"

%InterpretatorPath% %ScriptPath% "" "" "CompressResultShapes"

pause
