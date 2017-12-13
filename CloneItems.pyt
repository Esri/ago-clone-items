"""
-------------------------------------------------------------------------------
 | Copyright 2017 Esri
 |
 | Licensed under the Apache License, Version 2.0 (the "License");
 | you may not use this file except in compliance with the License.
 | You may obtain a copy of the License at
 |
 |    http://www.apache.org/licenses/LICENSE-2.0
 |
 | Unless required by applicable law or agreed to in writing, software
 | distributed under the License is distributed on an "AS IS" BASIS,
 | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 | See the License for the specific language governing permissions and
 | limitations under the License.
 ------------------------------------------------------------------------------
 """

import arcpy, clone_items
from arcgis import gis

class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "Clone Items"
        self.alias = "clone"

        # List of tool classes associated with this toolbox
        self.tools = [CloneItems]

class CloneItems(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Clone Items"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        param0 = arcpy.Parameter(
            displayName="Source Organization",
            name="source",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=False)
        param0.value = "http://www.arcgis.com/"

        param1 = arcpy.Parameter(
            displayName="Username",
            name="username",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
            multiValue=False)

        param2 = arcpy.Parameter(
            displayName="Password",
            name="password",
            datatype="GPStringHidden",
            parameterType="Optional",
            direction="Input",
            multiValue=False)

        param3 = arcpy.Parameter(
            displayName="Items",
            name="items",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
            multiValue=True)

        param4 = arcpy.Parameter(
            displayName="Output Folder",
            name="folder",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")

        param5 = arcpy.Parameter(
            displayName="Copy Data",
            name="copy_data",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param5.value = False

        param6 = arcpy.Parameter(
            displayName="Use Existing Items in Organization",
            name="search_for_existing_items",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param6.value = True

        param7 = arcpy.Parameter(
            displayName="Use Organization's Default Basemap",
            name="use_default_basemap",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param7.value = False

        param8 = arcpy.Parameter(
            displayName="Add GPS Metadata Fields",
            name="add_gps_metadata_fields",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        param8.value = False

        params = [param0, param1, param2, param3, param4, param5, param6, param7, param8]
        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        arcpy.env.autoCancelling = False

        #Setup the source portal
        source_url = parameters[0].valueAsText
        username = parameters[1].valueAsText
        password = parameters[2].valueAsText
        source = None
        try:
            source = gis.GIS(source_url, username, password)
        except ImportError:
            arcpy.AddError("Unable to connect to the source portal.\nIf you are using IWA authentication the pywin32 and kerberos-sspi packages are required.\nPlease install them using the following command:\n\tconda install kerberos-sspi\n")
        except:
            arcpy.AddError("Unable to connect to the source portal. Please ensure you provided the correct url and credentials.\n")
        if not source:
            return
        
        # Setup the target portal using the active portal within Pro
        target = None
        try:          
            target = gis.GIS('pro')
        except ImportError:
            arcpy.AddError("Unable to connect to the active portal.\nIf you are using IWA authentication the pywin32 and kerberos-sspi packages are required.\nPlease install them using the following command:\n\tconda install kerberos-sspi\n")
        except:
            arcpy.AddError("Unable to connect to the active portal. Please ensure you are logged into the active portal.\n")
        if not target:
            return        

        # Get the input item ids
        value_table = parameters[3].value
        item_ids = []
        for i in range(0, value_table.rowCount):
            item_id = value_table.getValue(i, 0)
            if item_id not in item_ids:
                item_ids.append(item_id) 
                
        # Get the output folder  
        output_folder = parameters[4].valueAsText
        if output_folder == '':
            output_foler = None

        # Set the global variables
        clone_items.COPY_DATA = parameters[5].value
        clone_items.SEARCH_ORG_FOR_EXISTING_ITEMS = parameters[6].value
        clone_items.USE_DEFAULT_BASEMAP = parameters[7].value
        clone_items.ADD_GPS_METADATA_FIELDS = parameters[8].value

        # Set the item extent if specified
        clone_items.ITEM_EXTENT = None
        if arcpy.env.extent is not None:
            extent = arcpy.env.extent
            extent_wgs84 = extent.projectAs(arcpy.SpatialReference(4326))
            clone_items.ITEM_EXTENT = '{0},{1},{2},{3}'.format(extent_wgs84.XMin, extent_wgs84.YMin, 
                                                  extent_wgs84.XMax, extent_wgs84.YMax)

        # Set the output spatial reference of any new feature services
        clone_items.SPATIAL_REFERENCE = None     
        if arcpy.env.outputCoordinateSystem is not None:
            clone_items.SPATIAL_REFERENCE = arcpy.env.outputCoordinateSystem.factoryCode

        # Loop through each item and clone it to the target portal
        created_items = []
        for item_id in item_ids:
            try:
                item = source.content.get(item_id)
            except RuntimeError as e:
                arcpy.AddError("Failed to get item {0}: {1}".format(item_id, str(e)))
                arcpy.AddMessage('------------------------')
                continue

            deploy_message = 'Cloning {0}'.format(item['title'])
            arcpy.AddMessage(deploy_message)
            arcpy.SetProgressor('default', deploy_message)                           

            created_items += clone_items.clone(target, item, output_folder, created_items)
            if arcpy.env.isCancelled:
                break


