import re
import urllib.parse as up
from typing import List, Set, Dict, Any
from ndp_ep import APIClient
from pathlib import Path
from kafka import KafkaAdminClient


def same_host(url_a: str, url_b: str) -> bool:
    """Return True if url_b is on the same hostname as url_a."""
    pa = up.urlparse(url_a)
    pb = up.urlparse(url_b)
    return pa.netloc.lower() == pb.netloc.lower()

def is_directory_link(href: str) -> bool:
    """Heuristic: treat trailing '/' as a directory link."""
    return href.endswith('/')

def is_allowed_file(href: str, allowed_ext: Set[str]) -> bool:
    """Return True if href ends with one of the allowed extensions."""
    path = up.urlparse(href).path
    for ext in allowed_ext:
        if path.lower().endswith(ext.lower()):
            return True
    return False

def extract_years_from_filename(url: str) -> List[int]:
    """Return all 4-digit years found in the filename part of the URL."""
    path = up.urlparse(url).path
    fname = path.split('/')[-1]
    years = re.findall(r'(?:19|20)\d{2}', fname)
    return [int(y) for y in years]

def should_register(url: str, min_year: int, max_year: int) -> bool:
    years = extract_years_from_filename(url)
    if not years:
        return False
    return min(years) >= min_year and max(years) <= max_year



def generate_url(url_list: List[str], min_year: int, max_year: int) -> List[str]:
    filtered_urls = [u for u in url_list if should_register(u, min_year, max_year)]
    # print(f"Files containing {min_year} <= year <= {max_year} : {len(filtered_urls)}\n")
    return filtered_urls


def generate_resource_name(url: str) -> str:
    path = up.urlparse(url).path
    fname = path.split('/')[-1] or "resource"
    # lowercase + ascii-only (drop non-ascii)
    fname = fname.encode("ascii", "ignore").decode("ascii").lower()
    # replace any disallowed char with '-'
    fname = re.sub(r'[^a-z0-9_-]+', '-', fname)
    # collapse repeats and trim separators
    fname = re.sub(r'[-_]{2,}', '-', fname).strip('-_')
    # fallback if empty after sanitization
    if not fname:
        fname = "resource"
    return fname


def generate_resource_title(url: str) -> str:
    path = up.urlparse(url).path
    fname = path.split('/')[-1]
    title = fname.replace('_', ' ').replace('.csv', '')
    return f"Sensor Data – {title}"

def generate_file_type(url: str) -> str:
    return Path(url).suffix.lstrip(".").upper() or "UNKNOWN"


def generate_description_for_file_from_url(url: str) -> str:
    parts = []

    # VEHICLE:
    # 1) BUS/TRX/TRAIN/RAIL with a required numeric id (e.g., BUS01, TRX03, TRAIN02, RAIL1)
    # VEHICLE:
    # BUS and RAIL require numeric IDs; TRX may appear with or without an ID.
    m_bus  = re.search(r'(?:^|[/_])BUS(?P<id>\d+)(?=[_.\/]|$)', url, re.IGNORECASE)
    m_trx  = re.search(r'(?:^|[/_])TRX(?P<id>\d*)(?=[_.\/]|$)', url, re.IGNORECASE)  # id optional
    m_rail = re.search(r'(?:^|[/_])RAIL(?P<id>\d+)(?=[_.\/]|$)', url, re.IGNORECASE)

    if m_bus:
        parts.append(f"Vehicle: Bus {m_bus.group('id')}")
    elif m_trx:
        tid = m_trx.group('id')
        parts.append(f"Vehicle: Train {tid}" if tid else "Vehicle: Train")
    elif m_rail:
        parts.append(f"Vehicle: Rail {m_rail.group('id')}")
    else:
        # EBUS: optional number; if digits follow, include them
        m = re.search(r'(?:^|[/_])EBUS(?P<id>\d*)(?=[_.\/]|$)', url, re.IGNORECASE)
        if m:
            eid = m.group('id')
            parts.append(f"Vehicle: E-bus{(' ' + eid) if eid else ''}")
        else:
            parts.append("Vehicle: Unknown")

    # DATE: try exact range first (YYYYMMDDHHMM_YYYYMMDDHHMM), then monthly (YYYY_MM)
    m_range = re.search(r'_(\d{12})_(\d{12})(?=[^0-9]|$)', url)
    m_month = re.search(r'_(\d{4})_(\d{2})(?=[^0-9]|$)', url)

    if m_range:
        start, end = m_range.groups()
        start_fmt = f"{start[:4]}-{start[4:6]}-{start[6:8]} {start[8:10]}:{start[10:12]}"
        end_fmt   = f"{end[:4]}-{end[4:6]}-{end[6:8]} {end[8:10]}:{end[10:12]}"
        parts.append(f"Data period: {start_fmt} → {end_fmt}")
    elif m_month:
        year, month = m_month.groups()
        parts.append(f"Data period: {year}-{month}")
    else:
        parts.append("Data period: Unknown")

    # FILE TYPE
    parts.append(f"File type: {generate_file_type(url)}")

    # FLAGS
    low = url.lower()
    if "noqc" in low:
        parts.append("File marked 'noqc' (no quality control).")
    if re.search(r'(^|[/_])min([_/\.]|$)', low):
        parts.append("File marked 'min' appears to be minute-resolution of data.")
    if "/meop/" in low:
        parts.append("File marked 'meop' (Mobile Environment Observation Platform) where sensors are attached to UTA.")

    # PROCESSING LEVEL (Level 2 / Level 3)
    if re.search(r'(?<![a-z0-9])level[-_]?2(?![a-z0-9])', low):
        parts.append("Data processing level: Level 2 (modified on raw data)")
    elif re.search(r'(?<![a-z0-9])level[-_]?3(?![a-z0-9])', low):
        parts.append("Data processing level: Level 3 (modified on Level 2 data)")
    else:
        parts.append("Data processing level: data is not modified.")

    return f"This dataset is available at {url}. " + " ".join(parts)



def generate_payloads(filtered_urls: List[str], org_name: str) -> List[Dict[str, Any]]:
    return [
        {
            'resource_name': generate_resource_name(u),
            'resource_title': generate_resource_title(u),
            'type': 'url',
            'resource_url': u,
            'notes': generate_description_for_file_from_url(u),
            'file_type': generate_file_type(u),
            'owner_org': org_name,
        }
        for u in filtered_urls
    ]

def register_csv(payloads, client: APIClient = None, SERVER: str = None) -> List[str]:
    """Register URL-based data objects in scidx_streaming.
    Replace the body with your actual scidx_streaming client calls.
    """
    ids = []
    
    for meta in payloads:
        try:
            response = client.register_url(meta, server=SERVER)
            print(response)
            ids.append(response["id"])
        except Exception as e:
            print(str(e)) 
    return ids

def update_csv_resource(resource_id: str, topic: str, client: APIClient = None, BOOTSTRAP: str = None, SERVER: str = None) -> dict:
        payload = {
            "topic": topic,
            "status": "active",
            "format": "kafka",
            "url": BOOTSTRAP,
            "description": f"Kafka stream for topic {topic}. This is a general stream without any filters.",
            "name": f"stream_dataset {topic}"
        }
        try:
            response = client.patch_general_dataset(
                dataset_id=resource_id,
                server=SERVER,
                data={"resources": [payload]}
            )
        except Exception as e:
            response = {"error": str(e)}
        return response

def register_kafka(urls: List[str] = None, org_name: str = "stream-simulation-ebus", client: APIClient = None, BOOTSTRAP: str = None, KAFKA_HOST: str = None, KAFKA_PORT: str = None, SERVER: str = None):
    """Register URL-based data objects in scidx_streaming.
    Replace the body with your actual scidx_streaming client calls.
    """

    admin_client = KafkaAdminClient(
    bootstrap_servers=BOOTSTRAP,
    client_id="topic_lister"
    )

    all_topics = admin_client.list_topics()
    for url in urls:
        topic = generate_resource_name(url)
        title = generate_resource_title(url)

        if topic not in all_topics:
            print(f"Topic '{topic}' does not exist in Kafka. Skipping registration.")
            continue
        # Define the payload data for the Kafka topic registration
        kafka_stream_metadata = {
            "dataset_name": f"kafka_{topic}",
            "dataset_title": f"Kafka {title}",
            "owner_org": org_name,
            "kafka_topic": topic,
            "kafka_host": KAFKA_HOST,
            "kafka_port": KAFKA_PORT,
            "dataset_description": f"The kafka stream is generated from csv dataset. {generate_description_for_file_from_url(url)}",
            # "extras": {
            #     "auto_offset_reset": "latest",
            # },
            "mapping":{
                # "time": "Timestamp",
                # "lat": "Latitude",
                # "lon": "Longitude",
                # "pm1": "ES405_PM1_Concentration",
                # "pm2.5": "ES405_PM2.5_Concentration",
                # "pm4": "ES405_PM4_Concentration",
                # "pm10": "ES405_PM10_Concentration",
                # "o3": "2B_Ozone_Concentration",
                # "pm2.5_flag": "PM2.5_Data_Flagged",
                # "o3_flag": "Ozone_Data_Flagged"
                # "time": "times",
                # "pm2.5": "PM2.5",
                # "pm10": "PM10",
                # "o3": "O3",
                # "pmf": "PMF",
                # "o3f": "O3F"
            }
        }

        # Call the register_kafka_topic method to add the Kafka topic
        try:
            response = client.register_kafka_topic(kafka_stream_metadata, server=SERVER)
        except ValueError as e:
            print("Failed to register Kafka topic:", e)
            response = {"error": str(e)}
        finally:
            print(response)

