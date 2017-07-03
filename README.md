# ago-clone-items
Clone items between ArcGIS Online and ArcGIS Enterprise organizations.

This script and associated tool can be used to clone an item and its dependencies to the same or another ArcGIS Organization. For example if you clone a hosted web application created using Web AppBuilder or a Configurable App Template, the script will find the web map that is used by that web application and all the hosted feature layers used in the web map. It will then clone all of these items to the new organization and swizzle the paths in the web map and web application to point to the new layers. This creates a completely disconnected copy of the application, map and layers in the organization.

[Learn more](../../wiki) about the supported item types and considerations for the script.

## Features

* A [python module](clone_items.py) that can be used to clone items within an ArcGIS Online or ArcGIS Enterprise organization or between organizations.
* An [example script](example.py) that demonstrates how you can clone items between ArcGIS organizations from a standalone script.
* A [python toolbox](CloneItems.pyt) that provides a geoprocessing tool that can be used to clone items from an ArcGIS organization to the active Portal within ArcGIS Pro.

## Requirements

* [ArcGIS API for Python](https://developers.arcgis.com/python/)
* ArcGIS Online or ArcGIS Enterprise
* ArcGIS Pro (optional)

## Instructions

1. [Install the ArcGIS API for Python](https://developers.arcgis.com/python/guide/install-and-set-up/).
2. Download or Clone this repo.
3. Run the script.
   * As a standalone script, see the [example script](example.py) as a starting point.
   * From ArcGIS Pro as a script tool using the [CloneItems](CloneItems.pyt) toolbox. 
     * Within ArcGIS Pro select Add Toolbox from the Insert tab > Toolbox menu and browse to the CloneItems.pyt.
     * From the Analysis tab click Tools
     * From the Geoprocessing pane search for 'Clone Items' and open the Clone Items tool.
     * [Learn more](../../wiki#clone-items-tool) about the tool and its parameters.

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing

Copyright 2017 Esri

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

A copy of the license is available in the repository's [LICENSE](LICENSE) file.
