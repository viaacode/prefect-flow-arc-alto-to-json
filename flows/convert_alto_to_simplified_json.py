import json
import sys
from typing import Dict, List, Optional

import requests
import datetime
from dateutil.parser import parse as parsedate

from lxml import etree as ET
from urllib.parse import urlparse, urlunparse


# Helper function that removes third part of URL path
def rewrite_url(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip("/").split("/")

    if len(path_parts) >= 3:
        path_parts.pop(2)  # Remove the third segment

    new_path = "/" + "/".join(path_parts)
    new_url = urlunparse((parsed_url.scheme, parsed_url.netloc, new_path, "", "", ""))

    return new_url


class TextLine:
    def __init__(self, text: str, x: int, y: int, width: int, height: int):
        self.text = text
        self.x = x
        self.y = y
        self.width = width
        self.height = height

    def to_dict(self):
        return {
            "text": self.text,
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
        }


class SimplifiedAlto:
    def __init__(
        self,
        description: Dict[str, Optional[str]],
        text: Optional[List[TextLine]],
    ):
        self.description = description
        self.text = text

    def __str__(self):
        return json.dumps(self.to_dict())

    def to_transcript(self):
        return " ".join(line.text for line in self.text)

    def to_dict(self):
        return {
            "description": self.description,
            "text": [line.to_dict() for line in self.text] if self.text else None,
        }


def extract_text_lines_from_alto(alto_tree: ET.ElementTree) -> SimplifiedAlto:
    root = alto_tree.getroot()
    namespace = root.tag.split("}")[0].strip("{")
    alto_version = None

    # Fallback for XML that is not well-formed
    if namespace in (None, "alto"):
        # Try to get schemaLocation
        schemaLocation = root.attrib.get("xsi:schemaLocation")
        alto_version = (
            root.attrib.get("xsi:schemaLocation").split()[0].split("/")[-1]
            if schemaLocation is not None
            else None
        )
        namespace = ""
    elif not namespace.startswith(
        "http://www.loc.gov/standards/alto/",
    ):
        alto_version = None
    else:
        alto_version = namespace.split("/")[-1]

    def get_text_lines_v2(layout):
        text_lines = []
        for page in layout.findall(f".//{{{namespace}}}Page"):
            for print_space in page.findall(f".//{{{namespace}}}PrintSpace"):
                for text_block in print_space.findall(f".//{{{namespace}}}TextBlock"):
                    for text_line in text_block.findall(f".//{{{namespace}}}TextLine"):
                        for string in text_line.findall(f".//{{{namespace}}}String"):
                            content = string.attrib.get("CONTENT")
                            if content:
                                text_lines.append(
                                    TextLine(
                                        text=content,
                                        x=int(string.attrib.get("HPOS", 0)),
                                        y=int(string.attrib.get("VPOS", 0)),
                                        width=int(string.attrib.get("WIDTH", 0)),
                                        height=int(string.attrib.get("HEIGHT", 0)),
                                    ),
                                )
        return text_lines

    def get_text_lines_v3(layout):
        text_lines = []
        for page in layout.findall(f".//{{{namespace}}}Page"):
            for text_block in page.findall(f".//{{{namespace}}}*[@ID]"):
                for text_line in text_block.findall(f".//{{{namespace}}}TextLine"):
                    for string in text_line.findall(f".//{{{namespace}}}String"):
                        content = string.attrib.get("CONTENT")
                        if content:
                            text_lines.append(
                                TextLine(
                                    text=content,
                                    x=int(string.attrib.get("HPOS", 0)),
                                    y=int(string.attrib.get("VPOS", 0)),
                                    width=int(string.attrib.get("WIDTH", 0)),
                                    height=int(string.attrib.get("HEIGHT", 0)),
                                ),
                            )
        return text_lines

    def get_description():
        description_element = root.find(f".//{{{namespace}}}Description")
        if description_element is None:
            return {}

        ocr_processing = description_element.find(f".//{{{namespace}}}OCRProcessing")
        processing_step = (
            ocr_processing.find(f".//{{{namespace}}}ocrProcessingStep")
            if ocr_processing
            else None
        )
        file_name = description_element.findtext(f".//{{{namespace}}}fileName")
        if processing_step:
            processing_date_time = processing_step.findtext(
                f".//{{{namespace}}}processingDateTime",
            )
            processing_step_settings = processing_step.findtext(
                f".//{{{namespace}}}processingStepSettings",
            )
            software_creator = processing_step.findtext(
                f".//{{{namespace}}}softwareCreator",
            )
            software_name = processing_step.findtext(f".//{{{namespace}}}softwareName")
            software_version = processing_step.findtext(
                f".//{{{namespace}}}softwareVersion",
            )

            return {
                "width": root.find(f".//{{{namespace}}}Page").attrib["WIDTH"],
                "height": root.find(f".//{{{namespace}}}Page").attrib["HEIGHT"],
                **({"fileName": file_name} if file_name is not None else {}),
                **(
                    {"processingDateTime": processing_date_time}
                    if processing_date_time is not None
                    else {}
                ),
                **(
                    {"processingStepSettings": processing_step_settings}
                    if processing_step_settings is not None
                    else {}
                ),
                **(
                    {"softwareCreator": software_creator}
                    if software_creator is not None
                    else {}
                ),
                **(
                    {"softwareName": software_name} if software_name is not None else {}
                ),
                **(
                    {"softwareVersion": software_version}
                    if software_version is not None
                    else {}
                ),
            }
        return {}

    if alto_version == "ns-v2#" or alto_version is None:
        layout = root.find(f".//{{{namespace}}}Layout")
        text_lines = get_text_lines_v2(layout) if layout else []
    elif alto_version == "ns-v3#":
        layout = root.find(f".//{{{namespace}}}Layout")
        text_lines = get_text_lines_v3(layout) if layout else []
    else:
        raise ValueError(f"Unsupported Alto format: {namespace}")

    description = get_description()
    return SimplifiedAlto(description, text_lines)


def convert_alto_xml_url_to_simplified_json(url: str) -> SimplifiedAlto:
    response = requests.get(url)
    # TEMP FIX: rewrite URL without third path component and try again
    if not response.ok:
        url = rewrite_url(url)
        response = requests.get(url)

    response.raise_for_status()
    alto_tree = ET.ElementTree(
        ET.fromstring(response.content, ET.XMLParser(encoding="utf-8", recover=True)),
    )
    return extract_text_lines_from_alto(alto_tree)


def is_alto_modified(url: str):
    response = requests.head(url)
    if response.ok:
        return False  # If we can access the URL, assume it's not modified
    # TEMP FIX: rewrite URL without third path component and try again
    url = rewrite_url(url)
    response = requests.head(url)
    if not response.ok:
        return False  # If we can access the URL, assume it's not modified
    return True # If we can't access the URL, assume it's modified


if __name__ == "__main__":
    url = sys.argv[1]
    simplified_alto = convert_alto_xml_url_to_simplified_json(url)
    print(simplified_alto)
