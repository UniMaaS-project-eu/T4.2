# UniMaaS Cloud-based Resource Monitoring Engine for ADIENT Pilot —  README

This repository implements an end-to-end Cloud-based Resource Monitoring pipeline for a container fleet (Magnum Optimum, Europallet, B-Container), corresonding to the DPPs of the Adient pilot. Physical container state flows from simulated devices, through W3C Web-of-Things Virtual Objects (VOs) built on the Nephele `vo-wot` runtime, into Apache Kafka, and finally into an Eclipse BaSyx Asset Administration Shell (AAS) environment that exposes the passports and site relationships. A composite VO (cVO) aggregates the fleet and persists per-product time series in InfluxDB.

## Component Analysis

### Layer 1: Device Simulation Layer

VO runtime to simulate IIoT devices
- `./security_not/device_magnum/` - Simulates Magnum Optimum container
- `./security_not/device_europallet/` - Simulates Europallet container
- `./security_not/device_bcontainer/` - Simulates B-Container

### Layer 2: Virtual Object (VO) Digital Twin Layer

Web of Things (WoT) compatible VOs that each subsribes to its corresponding device VO and monitors lifecycle properties' updates (`status`, `condition`, `currentLocation`, `lastMaintenanceDate`, `lifecycleCarbonFootprint`, `numberOfUses`)
- `./security_not/vo_magnum/` 
- `./security_not/vo_europallet/` 
- `./security_not/vo_bcontainer/` 

### Layer 3: Composite VO (Fleet Aggregator)
 
Aggregates data from all three container VOs into a single fleet-level view wwriting time-series metrics to InfluxDB.

### Layer 4: Kafka Event Streaming & Bridge
 
#### Kafka Topics for real-time container lifecycle events
1. **container-magnum** - Magnum Optimum container events
2. **container-europallet** - Europallet container events
3. **container-bcontainer** - B-Container container events
4. **cvo-fleet-updates** - cVO lifecycle updates

#### Kafka → BaSyx Bridge
 
**Purpose**: Consume Kafka events and synchronize them to BaSyx AAS submodels
 
**File**: `./kafka_basyx_bridge/consumer.py

**Workflow**:
1. Connect to Kafka broker
2. Subscribe to container-* topics
3. For each message:
   a. Parse Kafka event
   b. Extract property name and value
   c. Map to BaSyx submodel element
   d. PATCH to BaSyx AAS API
   e. If RDF enabled: Update GraphDB triples

### Layer 5: BaSyx Asset Administration Shell (AAS)
**Services**:  `aas-env`, `aas-registry`, `sm-registry`, `aas-discovery`, `mongo`, `mosquitto`, `influxdb`, `telegraf`, `aas-web-ui`, `dashboard-api`, `kafka-aas-bridge`

Standardized Asset Administration Shell (AAS) digital representation of Adient containers following the BaSyx specification.

#### AAS Structure for containers
**Example File**: `./Basyx/aas/magnum_optimum_dpp_aas.json`

**Structure**:

**AAS Shell** (Top-level container)

1. **Submodel: DigitalNameplate**
2. **Submodel: ProductLifecycle** (Updated by Bridge)
3. **Submodel: CarbonFootprint**
4. **Submodel: ProductInstanceData** (Current State)

#### BaSyx API Endpoints
 
```
GET  /shells                              # List all AAS shells
GET  /shells/{aasId}                      # Get AAS shell
GET  /shells/{aasId}/submodels            # List submodels
GET  /shells/{aasId}/submodels/{smId}     # Get submodel
PATCH /shells/{aasId}/submodels/{smId}/submodel-elements/{elementId}  # Update property
```

### Layer 6: RDF Dataset Export

The T4.2 implementation includes a generic BaSyx-to-MSC RDF exporter that converts AAS data into MSC Ontology-compatible RDF graphs. Designed to work across all UniMaaS pilots (Adient, ANV, Aegean, Catone).


### Layer 7: InfluxDB Time-Series Metrics
 
Time-series database for container lifecycle metrics.


## Deployment Guide
 
### Prerequisites
 
**Software**:
- Docker 24.0+
- Docker Compose 2.20+
- Python 3.9+
- Git

**Network**:
- All services run in isolated Docker networks `basyx-net` and `kafka-tst_kafka-net`
- Ports: 8080- 8085, 8087 (VOs), 9090,9091,9095-9099 (catalogue), 3000 (BaSyx UI), 8086 (InfluxDB), 8089 (Kafka UI)

### Quick Start 
 
#### 1. Clone Repository
```bash
git clone https://github.com/UniMaaS-project-eu/T4.2.git
cd T4.2
git checkout master 
```
 
#### 2. Start Kafka and Kafka→BaSyx Bridge
```bash
docker compose -f ./kafka-tst/docker-compose.yml up -d 
```

#### 3. Start BaSyx
```bash
docker compose -f ./Basyx/docker-compose.yml up -d
```
 
#### 4. Start Virtual Objects
```bash
docker compose -f ./security_not/docker-compose.yml up -d
``` 
#### 5: Build the RDF Exporter Image 

```bash
cd ./Basyx

# IMPORTANT: Always include BOTH compose files
# The extension file (-f docker-compose-rdf-exporter.yml) depends on services 
# from the main file (-f docker-compose.yml)

docker compose -f docker-compose.yml -f docker-compose-rdf-exporter.yml build --no-cache basyx-rdf-exporter
```
#### 6: One-time export (on-demand)

```bash
UNIMAAS_PILOT=adient docker compose -f docker-compose.yml \
    -f docker-compose-rdf-exporter.yml \
    run --rm basyx-rdf-exporter

# Output: rdf-datasets/msc_dataset_<timestamp>.ttl
```