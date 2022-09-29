import os
import glob
import xml.etree.ElementTree as Etree
from googlesat.utils import downloader
from urllib.request import HTTPError
import posixpath

def _readXML(path:str, file:str) -> Etree.Element:
    """Reads XML file.
    Args:
        path (str): Path to file
        file (str): Name of the file plus extention
    Returns:
        Etree.Element: XML opened file
    """
    tree = Etree.parse(os.path.join(path, file))
    root = tree.getroot()

    return root

def get_manifest(link:str, scene:str, verbose:bool = False) -> str:
    """Get the manifest.xml file.

    Args:
        link (str): Link to file in GCP Cloud
        scene (str): Path to local image
        verbose (bool, optional): Print option. Defaults to False

    Returns:
        str: Path to XML file
    """

    manifest = os.path.join(scene, 'manifest.safe')
    manifest_url = posixpath.join(link, "manifest.safe")
    
    # Download manifest file
    if os.path.exists(manifest):
        os.remove(manifest)
    downloader(manifest_url, manifest, verbose = verbose)

    return manifest

def get_data(link:str, store:str, verbose:bool = False):
    """Creates and downloads a S2 scene (*.SAFE) from GCP cloud.

    Args:
        link (str): Link to *SAFE folder
        store (str): Path to store the *.SAFE folder
        verbose (bool, optional): Print option. Defaults to False
    """

    scene_name = os.path.basename(link)
    scene = os.path.join(store, scene_name)

    print(f"Getting scene {scene_name}...")

    if not os.path.exists(scene):
        os.makedirs(scene)

    manifest = get_manifest(link, scene, verbose = verbose)    
    xml_root = _readXML(os.path.split(manifest)[0], os.path.split(manifest)[1])
    files = xml_root.findall("./dataObjectSection/dataObject/*/fileLocation/[@href]")
    
    for file in files:
        path = file.attrib["href"].split("./")[1]
        if path.startswith("HTML"):
            pass
        else:
            filepath = os.path.join(scene, file.attrib["href"].split("./")[1])
            fileurl = posixpath.join(link, file.attrib["href"].split("./")[1])
            file_folder = os.path.split(filepath)[0]
            if not os.path.exists(file_folder):
                os.makedirs(file_folder)
            if not os.path.exists(filepath):
                try:
                    downloader(fileurl, filepath, verbose = verbose)
                except HTTPError as error:
                    print(f"Error while downloading {fileurl} [{error}]")
                    continue
    
    extras = ["AUX_DATA", "HTML", "rep_info"]
    for dir in extras:
        if not os.path.exists(os.path.join(scene, dir)):
            os.makedirs(os.path.join(scene, dir))
        
        if dir is "rep_info":
            metadata = glob.glob(os.path.join(scene, "MTD*.xml"))[0]
            xml_root = _readXML(os.path.split(metadata)[0], os.path.split(metadata)[1])
            processing_level = xml_root.findall(".//PROCESSING_LEVEL")[0].text
            if processing_level == "Level-2A":
                xsds = ["S2_PDI_Level-2A_Datastrip_Metadata.xsd", "S2_PDI_Level-2A_Tile_Metadata.xsd", "S2_User_Product_Level-2A_Metadata.xsd"]
                for xsd in xsds:
                    try:
                        url = posixpath.join(link, dir, xsd)
                        downloader(url, os.path.join(scene, dir, xsd), verbose = verbose)
                    except HTTPError as error:
                        print(f"Error while downloading {fileurl} [{error}]")
                        continue
            
            elif processing_level == "Level-1C":
                xsd = "S2_User_Product_Level-1C_Metadata.xsd"
                try:
                    downloader(os.path.join(link, dir, xsd), os.path.join(scene, dir, xsd), verbose = verbose)
                except HTTPError as error:
                        print(f"Error while downloading {fileurl} [{error}]")
                        continue