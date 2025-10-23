#!/usr/bin/env python3
"""
SciDX Streaming MCP Server
Integrates SciDX streaming capabilities with MCP protocol
"""

import json
import asyncio
import sys
from typing import Dict, List, Any, Optional
from fastmcp import FastMCP
import pandas as pd
import numpy as np
import logging
import os
from dotenv import load_dotenv
import requests
import folium
import tempfile
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
import socket

# Suppress verbose logging from streaming libraries
logging.basicConfig(level=logging.WARNING)

# Load environment variables
load_dotenv()

EARTHSCOPE_USERNAME = os.getenv("EARTHSCOPE_USERNAME", "your-username-here")
EARTHSCOPE_PASSWORD = os.getenv("EARTHSCOPE_PASSWORD", "your-password-here")

# Import SciDX libraries
try:
    from scidx_streaming import StreamingClient
    from ndp_ep import APIClient
    SCIDX_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SciDX libraries not available: {e}", file=sys.stderr)
    SCIDX_AVAILABLE = False

# Hardcoded configuration (as requested)
TOKEN="eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJWbFJfUjhMNUFZN0FvOU5FSDA0MW9kSUJMczBMSk52bnQxU0ZTaDFjVDQ0In0.eyJleHAiOjE3NjIwMTY5NDAsImlhdCI6MTc2MTIzOTM0MCwiYXV0aF90aW1lIjoxNzYxMjM5MzQwLCJqdGkiOiIzMTAzNzIwYi1lOGUwLTQ3MzMtOTM4NC1kNmYwMzU4YzZjZWUiLCJpc3MiOiJodHRwczovL2lkcC5uYXRpb25hbGRhdGFwbGF0Zm9ybS5vcmcvcmVhbG1zL05EUCIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiJmYzYyNDkyNS1lZjA5LTQ0N2QtYmYxNi0zNzgwNjY3OTkyNzUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ0b2tlbl9jbGllbnQiLCJzZXNzaW9uX3N0YXRlIjoiNzAyODc0OTUtYmFiMC00MzRmLThjMjUtMTZhMTY2NTZmMDFjIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyJodHRwczovL3Rva2VuLm5kcC51dGFoLmVkdSJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiZGVmYXVsdC1yb2xlcy1uZHAiLCJvZmZsaW5lX2FjY2VzcyIsImdyb3VwOm5kcF9lcC9jbGllbnQtYXphZHN1bWFpeWEwMEBnbWFpbC5jb20tdW5pdmVyc2l0eS1vZi11dGFoLXNjaWR4LXN1bWFpeWE6YWRtaW4iLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGdyb3Vwc19tYXBwZXIgcHJvZmlsZSBlbWFpbCIsInNpZCI6IjcwMjg3NDk1LWJhYjAtNDM0Zi04YzI1LTE2YTE2NjU2ZjAxYyIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJuYW1lIjoiU3VtYWl5YSBBemFkIiwiZ3JvdXBzIjpbImNsaWVudC1hemFkc3VtYWl5YTAwQGdtYWlsLmNvbS11bml2ZXJzaXR5LW9mLXV0YWgtc2NpZHgtc3VtYWl5YSJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhemFkc3VtYWl5YTAwQGdtYWlsLmNvbSIsImdpdmVuX25hbWUiOiJTdW1haXlhIiwiZmFtaWx5X25hbWUiOiJBemFkIiwiZW1haWwiOiJhemFkc3VtYWl5YTAwQGdtYWlsLmNvbSJ9.Ub2aEkD-VwUnMrW_ISpamvManIauK8RMvsC7KamOP7pvSyiiylpfuRNWpwhCgx6y0VEqN2HxBEuvykEpwk64MnKT9BCtklGe1F21-iF62-_rMV6hLNyqTRKALj5ZZxpGbQNSFitUFg77q3uHYqEQTv1RShIn-mEhNOOBnvYRsODRBva4IYKiyRbTg_vlwKHRlahb0aim5919pB1AK-wGdjMF84n3Ze7CFCSrxyf8syyzgoSoYCNes6prSG3xduhBgP_FXGP1b0mC9e8gvYlp4P74g_K8U9wxsljz77xnoQAWGxT2O20eVoYHK-bDSVAOxs-wTujJgE9rzVcYf41-jA"
API_URL = "http://10.244.2.206:8003/"

# Create MCP server
mcp = FastMCP("SciDX Streaming Server")

# Global variables to store clients and active streams
api_client = None
streaming_client = None
active_streams = {}  # Store active streams by topic name
active_consumers = {}  # Store active consumers by topic name
search_results_cache = {}  # Cache search results for cleanup

# Tool 0: Initialize clients
@mcp.tool()
def initialize_clients() -> str:
    """
    Initialize the API and Streaming clients. ALWAYS make sure that clients are initialized before using other tools.
    """
    global api_client, streaming_client
    
    if not SCIDX_AVAILABLE:
        return "❌ SciDX libraries not available. Please install scidx_streaming and ndp_ep"
    
    try:
        # Initialize the NDP endpoint client
        api_client = APIClient(base_url=API_URL, token=TOKEN)
        
        # Initialize the Streaming client
        streaming_client = StreamingClient(api_client)
        
        return f"✅ Clients initialized successfully. Streaming User ID: {streaming_client.user_id}"
    except Exception as e:
        return f"❌ Failed to initialize clients: {str(e)}"

# Tool 1: Register data from URL
@mcp.tool()
def register_data_url(api_stream_metadata: dict) -> str:
    """
    Register streaming data with the NDP endpoint client using a URL.
    
    Args:
        api_stream_metadata: Dictionary containing metadata for the stream registration.
                           Expected keys: resource_name, resource_title, owner_org, 
                           resource_url, file_type, notes, mapping
    
    Returns:
        Registration result message
    """
    try:
        if api_client is None:
            return "❌ API client not initialized. Please call initialize_clients first."
        
        # Validate required fields
        required_fields = ['resource_name', 'resource_title', 'owner_org', 'resource_url', 'file_type']
        missing_fields = [field for field in required_fields if field not in api_stream_metadata]
        
        if missing_fields:
            return f"❌ Missing required fields: {', '.join(missing_fields)}"
        
        result = api_client.register_url(api_stream_metadata)
        return f"✅ Data registered successfully: {result}"
        
    except ValueError as e:
        return f"❌ Registration failed: {str(e)}"
    except Exception as e:
        return f"❌ Unexpected error during registration: {str(e)}"
    
# Tool 1b: Register data from kafka topic
@mcp.tool()
def register_data_kafka(kafka_stream_metadata: dict) -> str:
    """
    Register streaming data with the NDP endpoint client using a Kafka topic.
    
    Args:
        kafka_stream_metadata: Dictionary containing metadata for the stream registration.
                           Expected keys: dataset_name, dataset_title, owner_org, kafka_topic, 
                           kafka_host, kafka_port, extras (dictionary where all keys and values are strings),
                           mapping
    
    Returns:
        Registration result message
    """
    try:
        if api_client is None:
            return "❌ API client not initialized. Please call initialize_clients first."
        
        # Validate required fields
        required_fields = ['dataset_name', 'dataset_title', 'owner_org', 'kafka_host', 'kafka_topic', 'kafka_port']
        missing_fields = [field for field in required_fields if field not in kafka_stream_metadata]
        
        if missing_fields:
            return f"❌ Missing required fields: {', '.join(missing_fields)}"
        
        result = api_client.register_kafka_topic(kafka_stream_metadata)
        return f"✅ Kafka data registered successfully: {result}"
        
    except ValueError as e:
        return f"❌ Registration failed: {str(e)}"
    except Exception as e:
        return f"❌ Unexpected error during registration: {str(e)}"

# Tool 2: Search for registered entry
@mcp.tool()
def search_datasets(query: str, server: str = "local") -> str:
    """
    Search for datasets using the NDP endpoint client.
    
    Args:
        query: Search query string
        server: Server to search on ("local" or "global")
    
    Returns:
        Search results as formatted string with all available metadata
    """
    try:
        if api_client is None:
            return "❌ API client not initialized. Please call initialize_clients first."
        
        if server not in ["local", "global"]:
            return "❌ Server parameter must be 'local' or 'global'"
        
        results = api_client.search_datasets(query, server=server)
        
        # Cache results for potential cleanup operations
        global search_results_cache
        search_results_cache[query] = results
        
        if not results:
            return f"🔭 No datasets found for query: '{query}' on {server} server"
        
        # Format results for display
        formatted_results = f"🔍 Found {len(results)} dataset(s) for query: '{query}' on {server} server:\n\n"
        
        for i, dataset in enumerate(results):
            formatted_results += f"Dataset {i+1}:\n"
            formatted_results += f"{'-'*50}\n"
            
            # Display all key-value pairs dynamically
            for key, value in dataset.items():
                # Handle different value types
                if isinstance(value, (dict, list)):
                    # For complex structures, use JSON formatting
                    formatted_value = json.dumps(value, indent=2)
                    formatted_results += f"  {key}:\n"
                    # Indent the JSON output
                    for line in formatted_value.split('\n'):
                        formatted_results += f"    {line}\n"
                elif isinstance(value, str) and len(value) > 100:
                    # Truncate long strings
                    formatted_results += f"  {key}: {value[:100]}...\n"
                else:
                    # Regular key-value pairs
                    formatted_results += f"  {key}: {value}\n"
            
            formatted_results += "\n"
        
        return formatted_results
        
    except Exception as e:
        return f"❌ Search failed: {str(e)}"

# Tool 3: Create datastream
@mcp.tool()
async def create_datastream(keywords: list, match_all: bool = True, filters: list = None) -> str:
    """
    Create a Kafka data stream from registered datasets with optional filtering. 
    
    Args:
        keywords: List of keywords to match against datasets
        match_all: If True, only datasets matching ALL keywords are selected
        filters: Optional list of filter expressions to apply to the stream. Note that the variables used in the filters will depend on the dataset mapping.
                Available filter types:
                - Column comparisons: "x > y", "x == 10"
                - Math operations: "x > 10*y", "x + y < 100"
                - IN operator: "station IN ['A', 'B', 'C']"
                - Conditional logic: "IF x > 20 THEN alert = High ELSE y = 10"
                - Logical operators: "IF x > 10 OR z = 20 THEN alert = High ELSE alert = Low"
                - Window-based: "IF window_filter(9, sum, x > 20) THEN alert = High"
                Examples:
                  ["x > 100"]
                  ["station IN ['SVIN.NC.LY_.20', 'P159.PW.LY_.00']"]
                  ["IF x > 10 THEN alert = High ELSE alert = Low"]
                  ["IF window_filter(5, mean, temp > 30) THEN status = Critical"]

    Returns:
        Stream creation result message
    """
    try:
        if streaming_client is None:
            return "❌ Streaming client not initialized. Please call initialize_clients first."
        
        if not keywords:
            return "❌ Keywords list cannot be empty"
        
        # Prepare filter semantics (empty list if no filters provided)
        filter_semantics = filters if filters is not None else []
        
        # Validate filter syntax (basic check)
        if filter_semantics:
            for i, filter_expr in enumerate(filter_semantics):
                if not isinstance(filter_expr, str):
                    return f"❌ Filter {i+1} must be a string. Got: {type(filter_expr).__name__}"
        
        # Create the Kafka stream with filters
        stream = await streaming_client.create_kafka_stream(
            keywords=keywords,
            match_all=match_all,
            filter_semantics=filter_semantics,
            username=os.getenv("EARTHSCOPE_USERNAME", None),
            password=os.getenv("EARTHSCOPE_PASSWORD", None),
        )
        
        # Store the stream for later reference
        topic = stream.data_stream_id
        global active_streams
        active_streams[topic] = stream
        
        # Build result message
        result = f"✅ Stream created successfully!\n🎯 Topic: {topic}\n🔑 Keywords: {keywords}\n📊 Match all: {match_all}"
        
        if filter_semantics:
            result += f"\n🔍 Filters applied ({len(filter_semantics)}):"
            for i, filter_expr in enumerate(filter_semantics, 1):
                result += f"\n   {i}. {filter_expr}"
        else:
            result += "\n🔍 Filters: None"
        
        return result
        
    except Exception as e:
        return f"❌ Stream creation failed: {str(e)}"

# Tool 4: Consume the streamed data
@mcp.tool()
def consume_streamed_data(topic: str) -> str:
    """
    Start consuming data from a Kafka stream.
    
    Args:
        topic: The topic/stream ID to consume from
    
    Returns:
        Consumer initialization result message
    """
    try:
        if streaming_client is None:
            return "❌ Streaming client not initialized. Please call initialize_clients first."
        
        if topic not in active_streams:
            return f"❌ Topic '{topic}' not found in active streams. Available topics: {list(active_streams.keys())}"
        
        # Start consuming the Kafka stream
        consumer = streaming_client.consume_kafka_messages(topic)
        
        # Store the consumer for later reference
        global active_consumers
        active_consumers[topic] = consumer
        
        return f"✅ Started consuming data from topic: {topic}\n📡 Consumer is now active and collecting data...\n⏱️  Note: It may take a few seconds for data to populate."
        
    except Exception as e:
        return f"❌ Failed to start consumer: {str(e)}"

# Tool 5: Display the dataframe of the consumer
@mcp.tool()
def display_consumer_dataframe(topic: str, max_rows: int = 10) -> str:
    """
    Display the current dataframe from a consumer.
    
    Args:
        topic: The topic/stream ID of the consumer
        max_rows: Maximum number of rows to display
    
    Returns:
        Dataframe content formatted as a string which should be formatted into a table before displaying
    """
    try:
        if topic not in active_consumers:
            return f"❌ No active consumer found for topic: '{topic}'. Available consumers: {list(active_consumers.keys())}"
        
        consumer = active_consumers[topic]
        df = consumer.dataframe
        
        if df is None or df.empty:
            return f"🔭 No data available yet for topic: {topic}. Consumer may still be collecting data..."
        

        # Pad all lists in each row to the same length
        def pad_row(row):
            max_len = max(len(val) if isinstance(val, list) else 1 for val in row)
            return pd.Series({
                col: (val if isinstance(val, list) else [val]) + [np.nan] * (max_len - len(val) if isinstance(val, list) else max_len - 1)
                for col, val in row.items()
            })

        # Apply padding and explode
        df_exploded = df.apply(pad_row, axis=1).explode(df.columns.tolist()).reset_index(drop=True)

        df = df_exploded

        # Format dataframe for display
        total_rows = len(df)
        display_rows = min(max_rows, total_rows)
        
        result = f"📊 Consumer Dataframe for topic: {topic}\n"
        result += f"📈 Total rows: {total_rows}\n"
        result += f"📋 Columns: {list(df.columns)}\n"
        result += f"🔍 Showing first {display_rows} rows:\n\n"
        
        # Convert dataframe to string representation
        result += df.head(max_rows).to_string(index=False)
        
        if total_rows > max_rows:
            result += f"\n\n... and {total_rows - max_rows} more rows"
        
        return result
        
    except Exception as e:
        return f"❌ Failed to display dataframe: {str(e)}"

# Tool 6: Stop data consumption
@mcp.tool()
def stop_data_consumption(topic: str) -> str:
    """
    Stop data consumption for a specific topic.
    
    Args:
        topic: The topic/stream ID to stop consuming
    
    Returns:
        Stop operation result message
    """
    try:
        if topic not in active_consumers:
            return f"❌ No active consumer found for topic: '{topic}'. Available consumers: {list(active_consumers.keys())}"
        
        consumer = active_consumers[topic]
        consumer.stop()
        
        # Remove from active consumers
        del active_consumers[topic]
        
        return f"✅ Stopped data consumption for topic: {topic}\n🛑 Consumer has been terminated and removed from active list."
        
    except Exception as e:
        return f"❌ Failed to stop consumer: {str(e)}"

# Tool 7: Delete kafka stream
@mcp.tool()
async def delete_kafka_stream(topic: str) -> str:
    """
    Delete a Kafka stream.
    
    Args:
        topic: The topic/stream ID to delete
    
    Returns:
        Deletion result message
    """
    try:
        if streaming_client is None:
            return "❌ Streaming client not initialized. Please call initialize_clients first."
        
        if topic not in active_streams:
            return f"❌ Topic '{topic}' not found in active streams. Available topics: {list(active_streams.keys())}"
        
        # Stop consumer first if it's still active
        if topic in active_consumers:
            consumer = active_consumers[topic]
            consumer.stop()
            del active_consumers[topic]
        
        # Delete the stream
        stream = active_streams[topic]
        await streaming_client.delete_stream(stream)
        
        # Remove from active streams
        del active_streams[topic]
        
        return f"✅ Successfully deleted Kafka stream: {topic}\n🗑️  Stream and any associated consumer have been removed."
        
    except Exception as e:
        return f"❌ Failed to delete stream: {str(e)}"

# Tool 8: Delete registered dataset
@mcp.tool()
def delete_registered_dataset(dataset_id: str = None, query: str = None) -> str:
    """
    Delete a registered dataset by ID or by searching with query.
    
    Args:
        dataset_id: Direct dataset ID to delete
        query: Search query to find dataset to delete (uses cached search results)
    
    Returns:
        Deletion result message
    """
    try:
        if api_client is None:
            return "❌ API client not initialized. Please call initialize_clients first."
        
        if dataset_id is None and query is None:
            return "❌ Either dataset_id or query must be provided"
        
        if dataset_id is None and query is not None:
            # Use cached search results
            global search_results_cache
            if query not in search_results_cache:
                return f"❌ No cached search results for query: '{query}'. Please search first using search_datasets tool."
            
            results = search_results_cache[query]
            if not results:
                return f"❌ No datasets found for query: '{query}'"
            
            # Use the first result's ID
            dataset_id = results[0].get('id')
            if not dataset_id:
                return f"❌ No ID found in search results for query: '{query}'"
        
        # Delete the dataset
        result = api_client.delete_resource_by_id(dataset_id)
        
        return f"✅ Successfully deleted registered dataset with ID: {dataset_id}\n🗑️  Dataset has been removed from the NDP endpoint system."
        
    except Exception as e:
        return f"❌ Failed to delete dataset: {str(e)}"

# Tool 9: Download consumer dataframe as CSV
@mcp.tool()
def download_consumer_dataframe_csv(topic: str, filename: str = None) -> str:
    """
    Download the current dataframe from a consumer as a CSV file.
    
    Args:
        topic: The topic/stream ID of the consumer
        filename: Optional filename for the CSV (default: topic_name_data.csv)
    
    Returns:
        Download result message with file path
    """
    try:
        if topic not in active_consumers:
            return f"❌ No active consumer found for topic: '{topic}'. Available consumers: {list(active_consumers.keys())}"
        
        consumer = active_consumers[topic]
        df = consumer.dataframe
        
        if df is None or df.empty:
            return f"❌ No data available yet for topic: {topic}. Cannot create empty CSV."
        
        # Pad all lists in each row to the same length (same logic as display)
        def pad_row(row):
            max_len = max(len(val) if isinstance(val, list) else 1 for val in row)
            return pd.Series({
                col: (val if isinstance(val, list) else [val]) + [np.nan] * (max_len - len(val) if isinstance(val, list) else max_len - 1)
                for col, val in row.items()
            })

        # Apply padding and explode
        df_exploded = df.apply(pad_row, axis=1).explode(df.columns.tolist()).reset_index(drop=True)
        
        # Generate filename if not provided
        if filename is None:
            # Sanitize topic name for filename
            safe_topic = "".join(c if c.isalnum() or c in ('-', '_') else '_' for c in topic)
            filename = f"{safe_topic}_data.csv"
        
        # Ensure .csv extension
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        # Save to CSV
        df_exploded.to_csv(filename, index=False)
        
        # Get absolute path
        abs_path = os.path.abspath(filename)
        
        return f"✅ CSV file saved successfully!\n📁 File: {abs_path}\n📊 Rows: {len(df_exploded)}\n📋 Columns: {list(df_exploded.columns)}"
        
    except Exception as e:
        return f"❌ Failed to save CSV: {str(e)}"

# Tool 10: Filter GNSS stations by region
@mcp.tool()
def filter_gnss_stations_by_region(bounds: list) -> str:
    """
    Search for GNSS stations and filter them by geographic bounds.
    
    Args:
        bounds: List of two tuples defining the region bounds: [(lat_max, lon_min), (lat_min, lon_max)]
                Example: [(48.972940, -124.681449), (45.543541, -116.916031)]
                First tuple is northwest corner (max lat, min lon)
                Second tuple is southeast corner (min lat, max lon)
    
    Returns:
        List of station titles within the specified bounds. Use these titles for filters in create_datastream.
    """
    try:
        import requests

        # Validate bounds format
        if not isinstance(bounds, list) or len(bounds) != 2:
            return "❌ Invalid bounds format. Expected: [(lat_max, lon_min), (lat_min, lon_max)]"
        
        if not all(isinstance(b, (list, tuple)) and len(b) == 2 for b in bounds):
            return "❌ Each bound must be a tuple/list of 2 values: (latitude, longitude)"
        
        # Extract coordinates
        (lat_max, lon_min), (lat_min, lon_max) = bounds

        try:
            lat_min, lat_max = float(lat_min), float(lat_max)
            lon_min, lon_max = float(lon_min), float(lon_max)
        except (ValueError, TypeError):
            return "❌ Invalid coordinate values. All values must be numeric."
        
        # Validate bounds
        if lat_min >= lat_max:
            return "❌ Invalid latitude bounds: lat_min must be less than lat_max"
        if lon_min >= lon_max:
            return "❌ Invalid longitude bounds: lon_min must be less than lon_max"
        if not (-90 <= lat_min <= 90 and -90 <= lat_max <= 90):
            return "❌ Invalid latitude values: must be between -90 and 90"
        if not (-180 <= lon_min <= 180 and -180 <= lon_max <= 180):
            return "❌ Invalid longitude values: must be between -180 and 180"
        
        # Search for datasets
        url = f"{API_URL}search?terms=GNSS&server=global"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return f"❌ Search request failed with status code: {response.status_code}"
        
        data = response.json()
        
        if not data:
            return f"🔭 No datasets found.s"

        # Extract only title, latitude, and longitude
        filtered_data = []
        for item in data:
            filtered_data.append({
                'title': item.get('title'),
                'latitude': float(item['extras'].get('latitude')) if item.get('extras', {}).get('latitude') else None,
                'longitude': float(item['extras'].get('longitude')) if item.get('extras', {}).get('longitude') else None
            })

        # Convert to DataFrame for easy searching
        df = pd.DataFrame(filtered_data)

        # Clean up - remove rows with missing coordinates
        df = df.dropna()
        
        # Filter by geographic bounds
        filtered_df = df[
            (df['latitude'] <= lat_max) & (df['latitude'] >= lat_min) &
            (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max)
        ]
        
        if filtered_df.empty:
            return (f"🔭 No stations found within bounds:\n"
                   f"   Latitude: [{lat_min}, {lat_max}]\n"
                   f"   Longitude: [{lon_min}, {lon_max}]\n"
                   f"   (Searched {len(df)} total stations)")
        
        # Get list of titles
        station_titles = filtered_df['title'].tolist()
        
        # Format result
        result = f"📍 Found {len(station_titles)} station(s) within bounds:\n"
        result += ", ".join(station_titles)
        return result
        
    except requests.RequestException as e:
        return f"❌ Network error during search: {str(e)}"
    except Exception as e:
        return f"❌ Failed to filter stations: {str(e)}"


# Tool 11: Create map visualization of stations
@mcp.tool()
def create_stations_map(station_titles: list = None, bounds: list = None, filename: str = "map.html") -> str:
    """
    Create an interactive map visualization of GNSS stations.
    
    Args:
        station_titles: Optional list of specific station titles to display. If None, shows all stations.
        bounds: Optional region bounds to filter stations: [(lat_max, lon_min), (lat_min, lon_max)]
                If provided along with station_titles, only stations in both lists are shown.
                Use these coordinates for these regions: 
                    WASHINGTON = [(48.972940, -124.681449), (45.543541, -116.916031)]
                    OREGON = [(46.292035, -124.566406), (41.991794, -116.463623)]
                    CALIFORNIA = [(42.009518, -124.409637), (32.534156, -114.131211)]
                    SAN_DIEGO = [(33.114788, -117.282104), (32.534156, -116.463623)]
                    LOS_ANGELES = [(34.337306, -119.561768), (33.703652, -117.646484)]
                    SAN_FRANCISCO = [(38.334595, -123.173828), (37.639830, -121.716797)]
s
        filename: Optional output HTML filename. If None, auto-generates based on filters.
    
    Returns:
        Result message with hosted URL and map file location. 
    """
    try:        
        # Search for GNSS datasets
        url = f"{API_URL}search?terms=GNSS&server=global"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return f"❌ Search request failed with status code: {response.status_code}"
        
        data = response.json()
        
        if not data:
            return "🔭 No datasets found."
        
        # Extract title, latitude, and longitude
        filtered_data = []
        for item in data:
            extras = item.get('extras', {})
            lat = extras.get('latitude')
            lon = extras.get('longitude')
            
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = None
                lon = None
            
            if lat is not None and lon is not None:
                filtered_data.append({
                    'title': item.get('title', 'N/A'),
                    'latitude': lat,
                    'longitude': lon
                })
        
        if not filtered_data:
            return "⚠️ No stations found with valid coordinates"
        
        # Convert to DataFrame
        df = pd.DataFrame(filtered_data)
        
        # Filter by station titles if provided
        if station_titles:
            df = df[df['title'].isin(station_titles)]
            if df.empty:
                return f"❌ None of the specified stations found. Provided: {station_titles}"
        
        # Filter by bounds if provided
        if bounds:
            if not isinstance(bounds, list) or len(bounds) != 2:
                return "❌ Invalid bounds format. Expected: [(lat_max, lon_min), (lat_min, lon_max)]"
            
            try:
                (lat_max, lon_min), (lat_min, lon_max) = bounds
                lat_min, lat_max = float(lat_min), float(lat_max)
                lon_min, lon_max = float(lon_min), float(lon_max)
                
                df = df[
                    (df['latitude'] <= lat_max) & (df['latitude'] >= lat_min) &
                    (df['longitude'] >= lon_min) & (df['longitude'] <= lon_max)
                ]
                
                if df.empty:
                    return f"❌ No stations found within specified bounds"
            except (ValueError, TypeError) as e:
                return f"❌ Invalid bounds values: {e}"
        
        # Ensure .html extension
        if not filename.endswith('.html'):
            filename += '.html'
        
        # Create the map centered on the stations
        center_lat = df['latitude'].mean()
        center_lon = df['longitude'].mean()
        
        # Determine zoom level based on data spread
        lat_range = df['latitude'].max() - df['latitude'].min()
        lon_range = df['longitude'].max() - df['longitude'].min()
        max_range = max(lat_range, lon_range)
        
        if max_range > 10:
            zoom_start = 5
        elif max_range > 5:
            zoom_start = 6
        elif max_range > 2:
            zoom_start = 7
        elif max_range > 1:
            zoom_start = 8
        else:
            zoom_start = 9
        
        map_obj = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom_start,
            tiles='OpenStreetMap'
        )
        
        # Add markers for each station
        for idx, row in df.iterrows():
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=6,
                popup=folium.Popup(row['title'], max_width=300),
                tooltip=row['title'],
                color='blue',
                fill=True,
                fillColor='lightblue',
                fillOpacity=0.7,
                weight=2
            ).add_to(map_obj)
        
        # Add bounding box if bounds were provided
        if bounds:
            folium.Rectangle(
                bounds=[[lat_min, lon_min], [lat_max, lon_max]],
                color='red',
                fill=False,
                weight=2,
                dash_array='5, 5'
            ).add_to(map_obj)
        
        # Always create 'maps' in the same directory as this script
        project_root = os.path.dirname(os.path.abspath(__file__))
        maps_dir = os.path.join(project_root, "maps")
        os.makedirs(maps_dir, exist_ok=True)

        # Save to HTML file in maps directory
        filepath = os.path.join(maps_dir, filename)
        map_obj.save(filepath)
        
        # Get absolute path
        abs_path = os.path.abspath(filepath)

        # Start a simple HTTP server in the maps directory if not already running
        # Check if server is already running on port 8080
        def is_port_in_use(port):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                return s.connect_ex(('localhost', port)) == 0
            
        port = 8080
        if not is_port_in_use(port):
            def start_server():
                os.chdir(maps_dir)
                server = HTTPServer(('0.0.0.0', port), SimpleHTTPRequestHandler)
                server.serve_forever()

            # Start server in background thread
            server_thread = threading.Thread(target=start_server, daemon=True)
            server_thread.start()
            print(f"🌐 Started HTTP server on port {port}", file=sys.stderr)
        
        # Construct the URL
        # Get the server's hostname/IP
        hostname = socket.gethostname()
        try:
            host_ip = socket.gethostbyname(hostname)
        except:
            host_ip = "localhost"
        
        map_url = f"http://{host_ip}:{port}/{filename}"
        localhost_url = f"http://localhost:{port}/{filename}"
        
        result = f"✅ Map created successfully!\n\n"
        result += f"🌐 **View Map Online:**\n"
        result += f"   {map_url}\n"
        # result += f"   (or use {localhost_url} if accessing locally)\n\n"
        # result += f"📁 Local file: {abs_path}\n"
        result += f"📍 Stations plotted: {len(df)}\n"
        result += f"🗺️  Center: ({center_lat:.4f}, {center_lon:.4f})\n"
        
        if station_titles:
            result += f"🎯 Filtered by titles: {len(station_titles)} requested\n"
        if bounds:
            result += f"📦 Filtered by bounds: lat [{lat_min}, {lat_max}], lon [{lon_min}, {lon_max}]\n"
        
        result += f"\n💡 Click the URL above to view the interactive map in your browser!"
        
        return result
        
    except ImportError as e:
        return f"❌ Missing required library: {e}. Please install: pip install folium"
    except requests.RequestException as e:
        return f"❌ Network error during search: {str(e)}"
    except Exception as e:
        return f"❌ Failed to create map: {str(e)}"


# Add a resource to show system status
@mcp.resource("scidx://status")
def get_system_status() -> str:
    """Get the current status of the SciDX streaming system"""
    status = {
        "scidx_available": SCIDX_AVAILABLE,
        "api_client_initialized": api_client is not None,
        "streaming_client_initialized": streaming_client is not None,
        "streaming_user_id": streaming_client.user_id if streaming_client else None,
        "active_streams": list(active_streams.keys()),
        "active_consumers": list(active_consumers.keys()),
        "cached_search_queries": list(search_results_cache.keys())
    }
    
    return json.dumps(status, indent=2)

# Add a prompt for guided usage
@mcp.prompt()
def streaming_workflow_guide(use_case: str = "general") -> str:
    """
    Generate a step-by-step guide for using the SciDX streaming tools.
    
    Args:
        use_case: Type of workflow ("registration", "consumption", "cleanup")
    """
    guides = {
        "registration": """
Here's how to register and set up a new data stream:

1. **Initialize Clients**: Use initialize_clients() first
2. **Register Data**: Use register_data_url() or register_data_kafka() with metadata like:
    For register_data_url():
    {
        "resource_name": "my_stream_name",
        "resource_title": "My Stream Title", 
        "owner_org": "my_organization",
        "resource_url": "https://api.example.com/stream",
        "file_type": "stream",
        "notes": "Description of the stream",
        "mapping": {
        "timestamp": "timestamp",
        "value": "data_value"
        }
    }
    For register_data_kafka():
    {
     "dataset_name": "my_stream_name",
     "dataset_title": "My Stream Title",
     "owner_org": "my_organization",
     "kafka_topic": "my_topic_name",
     "kafka_host": "kafka.example.com",
     "kafka_port": 9092,
     "extras": {"sasl_mechanism": "PLAIN", 
                "security_protocol": "PLAINTEXT",
                "auto_offset_reset": "latest",
                "batch_interval": "1"}, # Note that batch_interval is string!
     "notes": "Description of the stream",
     "mapping": {
        "x": "coor[0]",
        "y": "coor[1]",
        "z": "coor[2]",
        "time": "time",
        "station": "SNCL",
     }
   }

3. **Verify Registration**: Use search_datasets() to confirm it was registered
4. **Create Stream**: Use create_datastream() with keywords that match your registered data and filters if needed. 
5. **Start Consuming**: Use consume_streamed_data() with the topic name
6. **Monitor Data**: Use display_consumer_dataframe() to see incoming data
        """,
        
        "consumption": """
Here's how to consume data from existing streams:

1. **Initialize Clients**: Use initialize_clients() first
2. **Search Available Data**: Use search_datasets() to find registered streams
3. **Create Stream**: Use create_datastream() with relevant keywords
4. **Start Consumer**: Use consume_streamed_data() with the topic ID
5. **Monitor Progress**: Use display_consumer_dataframe() to view data
6. **Check Status**: Use the scidx://status resource to see active streams
        """,
        
        "cleanup": """
Here's how to properly clean up resources:

1. **Stop Consumers**: Use stop_data_consumption() for each active topic
2. **Delete Streams**: Use delete_kafka_stream() for each active stream
3. **Remove Datasets**: Use delete_registered_dataset() to remove from NDP
4. **Verify Cleanup**: Check scidx://status to confirm everything is cleaned up
        """
    }
    
    return guides.get(use_case, f"Please provide a step-by-step guide for {use_case} using the SciDX streaming tools.")

if __name__ == "__main__":
    # Run the MCP server
    print("Starting SciDX MCP Server...", file=sys.stderr)
    # mcp.run(transport="http", host="127.0.0.1", port=8000)
    # mcp.run()
    mcp.run(transport="stdio")