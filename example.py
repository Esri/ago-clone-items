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
from arcgis import gis
import clone_items

def main():
    # Specify the source organization containing the items to clone.
    # If the item to be cloned is shared with everyone and is on arcgis online you don't need to change anything below.
    # If the item is on a local portal or not shared you need to provide the url to the portal and the username and password. 
    source = gis.GIS()

    # Specify the target portal, provide the url and the username/password to the organization or portal you want to clone the items to.
    # To use the active portal in Pro set the first parameter to 'pro'
    target = gis.GIS('http://www.arcgis.com/', '', '')

    # Provide the list of item's ids to clone. 
    # If it is an application or map, you don't need to provide the item id's of the hosted feature layers used by the map or the map used by the application.
    # These item dependencies will automatically be cloned as well.
    item_ids = ['c43607465cdb11e7907ba6006ad3dba0', 'fb1942765cfb11e7907ba6006ad3dba0']

    # Optionally specify whether the data from the original feature service or feature collection should be copied to the cloned item.
    # The default is False 
    clone_items.COPY_DATA = False

    # Optionally specify whether to search the org to see if the item has already been cloned, and if so use it instead of cloning a duplicate.
    # This is common if you have multiple web maps that share the same service. Rather than creating the service again it will find the already cloned version. 
    # If multiple versions of the item have been cloned to the org, the most recently created item will be used.
    # The default is True 
    clone_items.SEARCH_ORG_FOR_EXISTING_ITEMS = True

    # Optionally specify whether the organization's default basemap should be used when creating new web maps.
    # The default is False 
    clone_items.USE_DEFAULT_BASEMAP = False

    # Optionally specify whether gps metadata fields used by collector should be added to any new feature services.
    # The default is False 
    clone_items.ADD_GPS_METADATA_FIELDS = False

    # Optionally provide an extent to set the output extent of the cloned items.
    # A string representing the desired extent for new items. The string should be formatted as 'XMin, YMin, XMax, YMax' and the coordinates should be in WGS84. 
    # Example '-180, -90, 180, 90'. If not provided the new item will have the same extent as the original item.
    # The default is None 
    clone_items.ITEM_EXTENT = None

    # Optionally provide a well known id, for example 3857 for Web Mercator, to set the output spatial reference of any cloned feature services. 
    # This parameter requires the arcpy module. If not provided or if the arcpy module is not available the service will have the same spatial reference as the original service.
    # The default is None 
    clone_items.SPATIAL_REFERENCE = None

    created_items = []
    for item_id in item_ids:
        # Get the item
        item = source.content.get(item_id)
        print('Cloning {0}'.format(item['title']))
            
        # Specify the name of the folder to clone the items to. If a folder by the name doesn't already exist a new folder will be created.
        folder_name = "Output"

        # Clone the item to the target portal. The function will return all the new items that were created during the cloning. 
        created_items += clone_items.clone(target, item, folder_name, created_items)

if __name__ == "__main__":
    main()
