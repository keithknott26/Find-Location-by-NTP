#!/usr/bin/env python3

import time
import re
import requests
import ntplib
import concurrent.futures
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import sys
import math  # For bearing calculation
import socket
from datetime import datetime
import threading
from urllib.parse import urlparse

# --- Configuration ---
MAX_SERVERS = None
MAX_ITERATIONS = 100
DELAY_SECONDS = 60                # Delay between iterations (in seconds)
NTP_ATTEMPTS_PER_ITER = 5         # Attempts per iteration for NTP
TCP_ATTEMPTS_PER_ITER = 5         # Attempts per iteration for TCP handshake
HTTP_ATTEMPTS_PER_ITER = 5        # Attempts per iteration for HTTP request
MAX_GEOLOCATION_THREADS = 2       # For geolocation queries
RATE_LIMIT_DELAY = 10             # Seconds to pause if rate limited

# Hardcoded server lists for TCP and HTTP measurements
TCP_SERVERS = [
    ("google.com", 80),
    ("cloudflare.com", 80),
    ("amazon.com", 80),
    ("facebook.com", 80),
    ("youtube.com", 80),
    ("github.com", 80),
    ("microsoft.com", 80),
    ("fidelity.com", 80),
]

HTTP_SERVERS = [
    "https://www.google.com",
    "https://www.cloudflare.com",
    "https://www.amazon.com",
    "https://www.facebook.com",
    "https://www.youtube.com",
    "https://www.github.com",
    "https://www.microsoft.com",
    "https://www.fidelity.com",
]

# --- Rate Limiter Class ---
class RateLimiter:
    def __init__(self, max_calls, period):
        self.max_calls = max_calls
        self.period = period
        self.calls = []

    def can_call(self):
        now = time.time()
        self.calls = [call for call in self.calls if call > now - self.period]
        return len(self.calls) < self.max_calls

    def wait_if_needed(self):
        if not self.can_call():
            time.sleep(2)

    def call(self):
        self.calls.append(time.time())

# Thread‑Local Rate Limiter for ip‑api
_thread_local = threading.local()
def get_thread_ip_api_limiter():
    if not hasattr(_thread_local, 'ip_api_limiter'):
        _thread_local.ip_api_limiter = RateLimiter(40, 60)
    return _thread_local.ip_api_limiter

# --- Functions ---

def download_ntp_servers(url):
    print(f"Attempting to download NTP server list from: {url}")
    servers_found = set()
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()
        raw_text = response.text
        server_pattern = r'(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}|(?:(?:\d{1,3}\.){3}\d{1,3}))'
        server_regex = re.compile(server_pattern)
        lines = raw_text.splitlines()
        for line in lines:
            stripped_line = line.strip()
            if not stripped_line or stripped_line.startswith('#') or stripped_line.startswith('//') or \
               stripped_line.startswith('==') or stripped_line.startswith('--') or \
               stripped_line.startswith('**') or stripped_line.startswith('`'):
                continue
            match = server_regex.search(stripped_line)
            if match:
                found_server = match.group(0)
                first_word = stripped_line.split()[0].lstrip('*- ')
                if stripped_line.startswith(found_server) or first_word == found_server:
                    if '/' not in found_server:  # Avoid paths
                        servers_found.add(found_server)
        server_list = sorted(list(servers_found))
        if not server_list:
            print(f"Warning: No potential NTP servers extracted using line-based patterns from {url}.")
        else:
            print(f"Successfully parsed {len(server_list)} potential server addresses from this source.")
        return server_list
    except requests.exceptions.Timeout:
        print(f"Error: Request timed out while trying to reach {url}")
        return []
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP error occurred while fetching {url}: {e}")
        if "404" in str(e) and "gist.githubusercontent.com" in url:
            print("Suggestion: The Gist raw URL might be outdated. Verify the link manually and update url_gist_raw if needed.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to download NTP list from {url}: {e}")
        return []
    except re.error as e:
        print(f"Error: A regex error occurred during parsing: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

def get_ntp_data(server):
    c = ntplib.NTPClient()
    try:
        start_time = time.time()
        response = c.request(server, version=3, timeout=3)
        end_time = time.time()
        rtt = end_time - start_time
        offset = response.offset
        return server, rtt, offset
    except Exception:
        return server, None, None

def get_tcp_data(host, port):
    try:
        start_time = time.time()
        sock = socket.create_connection((host, port), timeout=3)
        end_time = time.time()
        sock.close()
        rtt = end_time - start_time
        return host, rtt, 0.0  # TCP handshake; no offset
    except Exception:
        return host, None, None

def get_http_data(url):
    try:
        start_time = time.time()
        response = requests.get(url, timeout=6)
        end_time = time.time()
        rtt = end_time - start_time
        hostname = urlparse(url).hostname
        return hostname, rtt, 0.0  # HTTP; no offset
    except Exception:
        hostname = urlparse(url).hostname
        return hostname, None, None

def multi_attempts_ntp(servers, attempts=NTP_ATTEMPTS_PER_ITER):
    data = {}
    active_servers = set(servers)
    for attempt in range(attempts):
        print(f"  NTP Query Attempt {attempt+1}/{attempts} (Max {len(active_servers)} servers)...", end='', flush=True)
        servers_to_query = list(active_servers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(get_ntp_data, s): s for s in servers_to_query}
            for future in concurrent.futures.as_completed(futures):
                s, rtt, offset = future.result()
                if rtt is not None and offset is not None:
                    if s not in data:
                        data[s] = []
                    data[s].append((rtt, offset))
        print(" Done.")
        time.sleep(1.0)
    return data

def multi_attempts_tcp(tcp_servers, attempts=TCP_ATTEMPTS_PER_ITER):
    data = {}
    for attempt in range(attempts):
        print(f"  TCP Query Attempt {attempt+1}/{attempts} (Total {len(tcp_servers)} servers)...", end='', flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(get_tcp_data, host, port): (host, port) for host, port in tcp_servers}
            for future in concurrent.futures.as_completed(futures):
                host, port = futures[future]
                s, rtt, offset = future.result()
                if rtt is not None:
                    if s not in data:
                        data[s] = []
                    data[s].append((rtt, offset))
        print(" Done.")
        time.sleep(1.0)
    return data

def multi_attempts_http(http_servers, attempts=HTTP_ATTEMPTS_PER_ITER):
    data = {}
    for attempt in range(attempts):
        print(f"  HTTP Query Attempt {attempt+1}/{attempts} (Total {len(http_servers)} servers)...", end='', flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(get_http_data, url): url for url in http_servers}
            for future in concurrent.futures.as_completed(futures):
                url = futures[future]
                s, rtt, offset = future.result()
                if rtt is not None:
                    if s not in data:
                        data[s] = []
                    data[s].append((rtt, offset))
        print(" Done.")
        time.sleep(1.0)
    return data

def _geolocate_server(server):
    """Helper function to geolocate a single server with fallback and per-thread rate limiting."""
    try:
        print(f"  [Thread] Locating {server}...", end='', flush=True)
        lat, lon = None, None
        response_ipapi = None
        response_geoplugin = None
        response_extremeip = None

        rl = get_thread_ip_api_limiter()
        rl.wait_if_needed()
        url_ipapi = f"http://ip-api.com/json/{server}?fields=status,message,lat,lon,query"
        attempts_ipapi = 0
        max_attempts_ipapi = 3
        while attempts_ipapi < max_attempts_ipapi:
            response_ipapi = requests.get(url_ipapi, timeout=6)
            if response_ipapi.status_code == 429:
                print(f" -> (ip-api) Rate limit exceeded. Pausing for {RATE_LIMIT_DELAY} seconds and retrying...")
                time.sleep(RATE_LIMIT_DELAY)
                attempts_ipapi += 1
            else:
                break
        if response_ipapi.status_code == 429:
            print(f" -> (ip-api) Still rate limited after {max_attempts_ipapi} attempts. Falling back to geoplugin...")
        else:
            response_ipapi.raise_for_status()
            try:
                data_ipapi = response_ipapi.json()
                rl.call()
                if data_ipapi.get("status") == "success":
                    lat = data_ipapi["lat"]
                    lon = data_ipapi["lon"]
                    print(f" -> (ip-api) ({lat:.4f}, {lon:.4f})")
                    return server, (lat, lon)
            except Exception as e_json_ipapi:
                print(f" -> (ip-api) JSON Decode Error: {e_json_ipapi}, Response: {response_ipapi.text if response_ipapi else 'No Response'}")
        # Fallback to geoplugin
        print(f" -> (ip-api) Retrying using geoplugin for {server}...")
        url_geoplugin = f"http://www.geoplugin.net/json.gp?ip={server}"
        try:
            response_geoplugin = requests.get(url_geoplugin, timeout=6)
            response_geoplugin.raise_for_status()
            try:
                data_geoplugin = response_geoplugin.json()
                if data_geoplugin.get('geoplugin_latitude') and data_geoplugin.get('geoplugin_longitude'):
                    try:
                        lat = float(data_geoplugin['geoplugin_latitude'])
                        lon = float(data_geoplugin['geoplugin_longitude'])
                        print(f" -> (geoplugin) ({lat:.4f}, {lon:.4f})")
                        return server, (lat, lon)
                    except Exception as e_parse:
                        print(f" -> (geoplugin) Error parsing lat/lon: {e_parse}, Response: {data_geoplugin}")
                        return server, None
                else:
                    print(f" -> (geoplugin) Failed to locate, trying extreme-ip-lookup...")
                    url_extremeip = f"https://extreme-ip-lookup.com/json/{server}"
                    try:
                        response_extremeip = requests.get(url_extremeip, timeout=6)
                        response_extremeip.raise_for_status()
                        try:
                            data_extremeip = response_extremeip.json()
                            if data_extremeip.get('latitude') and data_extremeip.get('longitude'):
                                try:
                                    lat = float(data_extremeip['latitude'])
                                    lon = float(data_extremeip['longitude'])
                                    print(f" -> (extreme-ip) ({lat:.4f}, {lon:.4f})")
                                    return server, (lat, lon)
                                except Exception as e_parse:
                                    print(f" -> (extreme-ip) Error parsing lat/lon: {e_parse}, Response: {data_extremeip}")
                                    return server, None
                            else:
                                print(f" -> (extreme-ip) Failed to locate.")
                                return server, None
                        except Exception as e_extreme:
                            print(f" -> (extreme-ip) Exception: {type(e_extreme)}, Response: {response_extremeip.text if response_extremeip else 'No Response'}")
                            return server, None
                    except Exception as e_json_extreme:
                        print(f" -> (extreme-ip) JSON Decode Error: {e_json_extreme}, Response: {response_extremeip.text if response_extremeip else 'No Response'}")
                        return server, None
            except Exception as e_json_geoplugin:
                print(f" -> (geoplugin) JSON Decode Error: {e_json_geoplugin}, Response: {response_geoplugin.text if response_geoplugin else 'No Response'}")
                return server, None
        except Exception as e:
            print(f" -> (geoplugin) Request Exception: {e}")
            return server, None
    except Exception as general_e:
        print(f" -> General exception in _geolocate_server for {server}: {type(general_e)}, Error: {general_e}")
        return server, None

def geolocate_servers(servers):
    locations = {}
    total_servers = len(servers)
    if total_servers == 0:
        return {}
    print(f"Attempting to geolocate {total_servers} servers using {MAX_GEOLOCATION_THREADS} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_GEOLOCATION_THREADS) as executor:
        futures = [executor.submit(_geolocate_server, server) for server in servers]
        for future in concurrent.futures.as_completed(futures):
            server, location = future.result()
            if location:
                locations[server] = location
            time.sleep(0.1)
    print(f"\nFinished geolocation. Successfully located {len(locations)} out of {total_servers} servers.")
    return locations

def estimate_location(measurements, server_coords):
    """
    Combines all measurements (each a tuple of (rtt, offset)) from various protocols.
    Uses a weight of 1/(rtt + |offset|) to weight the known server geolocations.
    """
    weighted_lat = 0
    weighted_lon = 0
    total_weight = 0
    usable_measurements = 0

    if not isinstance(measurements, dict):
        return None

    for server, meas_list in measurements.items():
        if server not in server_coords:
            continue
        server_weight = 0
        for rtt, offset in meas_list:
            if not isinstance(rtt, (int, float)) or rtt <= 0 or rtt > 10.0:
                continue
            weight = 1.0 / (rtt + abs(offset) + 1e-9)
            server_weight += weight
            usable_measurements += 1
        if server_weight > 0:
            lat, lon = server_coords[server]
            weighted_lat += lat * server_weight
            weighted_lon += lon * server_weight
            total_weight += server_weight

    if total_weight == 0 or usable_measurements < 3:
        return None

    est_lat = weighted_lat / total_weight
    est_lon = weighted_lon / total_weight
    est_lat = max(-90.0, min(90.0, est_lat))
    est_lon = max(-180.0, min(180.0, est_lon))
    return est_lat, est_lon

def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dLon = lon2 - lon1
    x = math.sin(dLon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dLon)
    initial_bearing = math.atan2(x, y)
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360
    return compass_bearing

def bearing_to_direction(bearing):
    if bearing is None:
        return "N/A"
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    index = round(bearing / (360. / len(dirs)))
    return dirs[index % len(dirs)]

def haversine(lat1, lon1, lat2, lon2):
    R = 3958.8  # Radius of Earth in miles
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    return distance

def plot_world_map(server_coords, location_history, ts):
    if not server_coords and not location_history:
        print("No server coordinates or location history to plot.")
        return

    fig = plt.figure(figsize=(15, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_global()
    ax.stock_img()
    ax.add_feature(cfeature.BORDERS, linestyle=':', alpha=0.7)
    ax.add_feature(cfeature.COASTLINE, alpha=0.8)
    ax.add_feature(cfeature.LAND, facecolor='#c9c9c9', alpha=0.6)
    ax.add_feature(cfeature.OCEAN, facecolor='#a6cae0', alpha=0.6)

    if server_coords:
        print(f"Plotting {len(server_coords)} server locations...")
        # Correct coordinate order: (lat, lon) -> (lon, lat)
        lats, lons = zip(*server_coords.values())
        ax.plot(lons, lats, 'o', color='red', markersize=3, alpha=0.4,
                transform=ccrs.Geodetic(), label='Servers', linestyle='')

    if location_history:
        print(f"Plotting {len(location_history)} estimated location points...")
        # location_history stores (lat, lon) so swap to (lon, lat)
        lats, lons = zip(*location_history)
        colors = plt.cm.coolwarm(np.linspace(0, 1, len(location_history)))
        if len(location_history) > 1:
            ax.plot(lons, lats, '-', color='white', linewidth=1.5, alpha=0.8,
                    transform=ccrs.Geodetic(), zorder=9)
        scatter = ax.scatter(lons, lats, c=colors, s=30,
                             transform=ccrs.Geodetic(), label='Est. Location (Time)',
                             edgecolor='black', linewidth=0.5, zorder=10)
        if len(location_history) >= 1:
            ax.plot(lons[0], lats[0], 'o', color='lime', markersize=8,
                    markeredgecolor='black', transform=ccrs.Geodetic(), zorder=11)
            ax.text(lons[0] + 2, lats[0] + 2,
                    ts[0].split('T')[1].split('Z')[0],
                    fontsize=9, color='black', weight='bold',
                    transform=ccrs.Geodetic(), zorder=12)
        if len(location_history) > 1:
            ax.plot(lons[-1], lats[-1], 'X', color='fuchsia', markersize=9,
                    markeredgecolor='black', transform=ccrs.Geodetic(), zorder=11)
            ax.text(lons[-1] + 2, lats[-1] - 3,
                    ts[-1].split('T')[1].split('Z')[0],
                    fontsize=9, color='black', weight='bold',
                    transform=ccrs.Geodetic(), zorder=12)
        if len(location_history) > 1:
            sm = plt.cm.ScalarMappable(cmap=plt.cm.coolwarm,
                                       norm=plt.Normalize(vmin=1, vmax=len(location_history)))
            sm.set_array([])
            cbar = plt.colorbar(sm, ax=ax, orientation='horizontal', pad=0.02,
                                shrink=0.5, aspect=30)
            cbar.set_label('Iteration Number')
            cbar.ax.tick_params(labelsize=8)

    plt.title("Location Estimation History")
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        plt.legend(loc='lower left', fontsize='small')

    ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False,
                 alpha=0.5, linestyle='--')

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    print("--- Starting Location Estimation ---")

    # Download NTP server lists
    url_gist_raw = "https://gist.githubusercontent.com/mutin-sa/eea1c396b1e610a2da1e5550d94b0453/raw/"
    url_repo_raw = "https://raw.githubusercontent.com/the29a/ntp-servers-list/master/ntp-servers-list.md"
    print("\n--- Downloading and Parsing NTP Server Lists ---")
    servers_gist = download_ntp_servers(url_gist_raw)
    servers_repo = download_ntp_servers(url_repo_raw)
    combined_ntp_servers = set(servers_gist) | set(servers_repo)
    ntp_servers = sorted(list(combined_ntp_servers))
    if not ntp_servers:
        print("\nNo NTP servers found from any source. Exiting.")
        sys.exit(1)
    print(f"\n--- Combined and deduplicated {len(ntp_servers)} NTP servers ---")

    if MAX_SERVERS is not None:
        limit = min(MAX_SERVERS, len(ntp_servers))
        if limit < len(ntp_servers):
            print(f"Limiting NTP server list to {limit} servers for testing.")
            ntp_servers = ntp_servers[:limit]
    print(f"Using {len(ntp_servers)} NTP servers for measurements.")

    # Combine hostnames for geolocation from NTP, TCP, and HTTP servers
    tcp_hostnames = [host for host, port in TCP_SERVERS]
    http_hostnames = [urlparse(url).hostname for url in HTTP_SERVERS]
    all_server_hostnames = set(ntp_servers) | set(tcp_hostnames) | set(http_hostnames)
    print(f"\n--- Geolocating {len(all_server_hostnames)} servers ---")
    server_locations = geolocate_servers(list(all_server_hostnames))
    if not server_locations:
        print("\nWarning: Failed to geolocate any servers. Location estimation may fail or be inaccurate.")

    # Main measurement loop
    location_history = []
    timestamps = []
    print(f"\n--- Starting Location Estimation Loop ({MAX_ITERATIONS} iterations, ~{DELAY_SECONDS}s interval) ---")
    print(f"--- (NTP attempts: {NTP_ATTEMPTS_PER_ITER}, TCP attempts: {TCP_ATTEMPTS_PER_ITER}, HTTP attempts: {HTTP_ATTEMPTS_PER_ITER}) ---")
    print("Press Ctrl+C to stop early.")

    try:
        for i in range(MAX_ITERATIONS):
            start_iter_time = time.time()
            print(f"\n===== Iteration {i+1}/{MAX_ITERATIONS} Start =====")

            ntp_data = multi_attempts_ntp(ntp_servers, attempts=NTP_ATTEMPTS_PER_ITER)
            tcp_data = multi_attempts_tcp(TCP_SERVERS, attempts=TCP_ATTEMPTS_PER_ITER)
            http_data = multi_attempts_http(HTTP_SERVERS, attempts=HTTP_ATTEMPTS_PER_ITER)

            # Combine measurements from all protocols (each measurement is a tuple of (rtt, offset))
            combined_measurements = {}
            for data_dict in [ntp_data, tcp_data, http_data]:
                for server, meas_list in data_dict.items():
                    if server not in combined_measurements:
                        combined_measurements[server] = []
                    combined_measurements[server].extend(meas_list)

            estimated_location = estimate_location(combined_measurements, server_locations)
            if estimated_location:
                timestamp_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                print(f"==> Iteration {i+1} Estimated location: Lat={estimated_location[0]:.4f}, Lon={estimated_location[1]:.4f} ({timestamp_utc})")
                location_history.append(estimated_location)
                timestamps.append(timestamp_utc)
            else:
                print(f"==> Iteration {i+1} Failed to estimate location.")

            if i < MAX_ITERATIONS - 1:
                elapsed_iter_time = time.time() - start_iter_time
                wait_time = max(0, DELAY_SECONDS - elapsed_iter_time)
                print(f"Iteration took {elapsed_iter_time:.1f}s. Waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)

    except KeyboardInterrupt:
        print("\n--- Loop interrupted by user ---")

    print("\n--- Final Analysis ---")
    successful_estimations = len(location_history)
    print(f"Completed {successful_estimations} successful location estimations out of {MAX_ITERATIONS} iterations.")

    direction = "N/A"
    bearing = None
    if successful_estimations >= 2:
        start_lat, start_lon = location_history[0]
        end_lat, end_lon = location_history[-1]
        dist_check = math.sqrt((end_lat - start_lat)**2 + (end_lon - start_lon)**2)
        if dist_check > 1e-4:
            bearing = calculate_bearing(start_lat, start_lon, end_lat, end_lon)
            direction = bearing_to_direction(bearing)
            print(f"Overall direction of travel (Start to End): {direction} ({bearing:.1f}°)")
        else:
            print("Start and End estimated locations are effectively the same. No direction calculated.")

        print("\nSpeed between consecutive points (mph):")
        for j in range(len(location_history) - 1):
            lat1, lon1 = location_history[j]
            lat2, lon2 = location_history[j+1]
            time1_str = timestamps[j]
            time2_str = timestamps[j+1]
            time1 = datetime.strptime(time1_str, '%Y-%m-%dT%H:%M:%SZ')
            time2 = datetime.strptime(time2_str, '%Y-%m-%dT%H:%M:%SZ')
            time_diff_seconds = (time2 - time1).total_seconds()
            if time_diff_seconds > 0:
                distance_miles = haversine(lat1, lon1, lat2, lon2)
                time_diff_hours = time_diff_seconds / 3600
                speed_mph = distance_miles / time_diff_hours
                print(f"  Point {j+1} to {j+2}: {speed_mph:.2f} mph")
            else:
                print(f"  Point {j+1} to {j+2}: Time difference is zero, cannot calculate speed.")
    else:
        print("Not enough data points (need >= 2) to determine direction or speed of travel.")

    if location_history:
        print("\nEstimated Coordinates per Iteration (UTC Timestamp):")
        for idx, (lat, lon) in enumerate(location_history):
            print(f"  Point {idx+1}: Lat={lat:.4f}, Lon={lon:.4f} ({timestamps[idx]})")

    print("\n--- Plotting World Map ---")
    plot_world_map(server_locations, location_history, timestamps)

    print("\n--- Script Finished ---")
