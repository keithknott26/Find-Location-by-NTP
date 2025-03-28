from random import shuffle
import time
from geopy.geocoders import Nominatim
import geopy

# URL containing list of NTP servers
URL = "https://gist.githubusercontent.com/mutin-sa/eea1c396b1e610a2da1e5550d94b0453/raw/c40d10e496a71fcb95ccc9878c421f0f3966e7de/Top_Public_Time_Servers.md"


def get_ntp_servers(url):
    import requests
    response = requests.get(url)
    content = response.content.decode("utf-8")
    # Parse content and extract NTP servers
    servers = []
    for line in content.splitlines():
        # Ignore comments and empty lines
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Extract properly formatted FQDN time servers
        parts = line
        servers.append(f"{line}")

    return servers[:75]  # Return only 25 servers


def get_time_drift(server):
    try:
        import random
        # Get server time
        server_time = time.time()
        # Simulate querying an NTP server by adding a random offset
        server_time += random.uniform(-0.1, 0.1)
        print(f"Server: {server}, Time: {server_time}")
    except Exception as e:
        # Server unreachable, skip
        print(f"Error querying server {server}: {e}")
        return None

    # Get local time
    local_time = time.time()
    print(f"Local time: {local_time}")

    # Calculate drift
    drift = server_time - local_time
    print(f"Drift: {drift}")
    return drift


def triangulate_location(data):
    geolocator = Nominatim(user_agent="NTP Triangulation")

    # Calculate speed of time signal (replace with actual value)
    propagation_speed = 299792458  # meters per second

    # Convert drift to distance using propagation speed
    distances = [abs(drift) * propagation_speed for drift in data]

    # Get locations from server addresses using geolocator
    locations = [geolocator.geocode(server_address) for server_address, _ in data]

    # Check if all locations are valid
    if not all(locations):
        raise ValueError("Failed to retrieve location for all servers")

    # Triangulate location using distances and locations
    # ... (Implement your triangulation algorithm here)


def main():
    # Get NTP servers
    ntp_servers = get_ntp_servers(URL)

    # Check if enough servers were found
    if len(ntp_servers) < 25:
        print(f"Error: Not enough servers found. Found {len(ntp_servers)}")
        return

    # Shuffle servers for random selection
    shuffle(ntp_servers)

    # Query 25 servers and collect data
    data = []
    for server in ntp_servers:
        drift = get_time_drift(server)
        if drift is not None:
            data.append((server, drift))

    # Print data for debugging
    print(f"Data: {data}")

    # Calculate master time drift
    master_drift = sum(drift for _, drift in data) / len(data)

    # Print master time drift
    print(f"Master time drift: {master_drift:.6f} seconds")

    # Attempt to triangulate location
    try:
        latitude, longitude = triangulate_location(data)
        # Print estimated location
        print(f"Estimated location: {latitude:.6f}, {longitude:.6f}")
    except Exception as e:
        print("Failed to triangulate location:", e)


if __name__ == "__main__":
    main()
