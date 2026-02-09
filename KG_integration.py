#BaSyx AAS to Neo4j Knowledge Graph Synchronization

import requests
import time
import logging
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
from datetime import datetime
import base64
import json
from urllib.parse import quote, unquote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BaSyxConfig:
    AAS_REGISTRY = "http://localhost:8082"
    SUBMODEL_REGISTRY = "http://localhost:8083"
    AAS_REPO = "http://localhost:8081"
    POLL_INTERVAL = 3  # seconds


class Neo4jConfig:
    URI = "bolt://localhost:7687"
    USER = "neo4j"
    PASSWORD = "fVcPu!d3iV#0cT"
    DATABASE = "neo4j"


class BaSyxClient:
    
    def __init__(self, config: BaSyxConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})
    
    # Base64URL encoding for BaSyx urls
    def _base64_url_encode(self, identifier: str) -> str:
        return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip('=')
    
    def get_all_aas_descriptors(self) -> List[Dict]:
        try:
            url = f"{self.config.AAS_REGISTRY}/shell-descriptors"
            response = self.session.get(url)
            response.raise_for_status()
            result = response.json()
            return result.get('result', [])
        except Exception as e:
            logger.error(f"Error fetching AAS descriptors: {e}")
            return []
    
    def get_aas_by_id(self, aas_id: str) -> Optional[Dict]:
        try:
            encoded_id = self._base64_url_encode(aas_id)
            url = f"{self.config.AAS_REPO}/shells/{encoded_id}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching AAS {aas_id}: {e}")
            return None
    
    def get_submodel_by_id(self, submodel_id: str) -> Optional[Dict]:
        try:
            encoded_id = self._base64_url_encode(submodel_id)
            url = f"{self.config.AAS_REPO}/submodels/{encoded_id}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching submodel {submodel_id}: {e}")
            return None
    
    def get_all_submodels_for_aas(self, aas_id: str) -> List[Dict]:
        submodels = []
        try:
            encoded_id = self._base64_url_encode(aas_id)
            url = f"{self.config.AAS_REPO}/shells/{encoded_id}/submodel-refs"
            response = self.session.get(url)
            response.raise_for_status()
            submodel_refs = response.json().get('result', [])
            
            for ref in submodel_refs:
                # Extract submodel ID from reference
                if 'keys' in ref and len(ref['keys']) > 0:
                    submodel_id = ref['keys'][0].get('value')
                    if submodel_id:
                        submodel = self.get_submodel_by_id(submodel_id)
                        if submodel:
                            submodels.append(submodel)
        except Exception as e:
            logger.error(f"Error fetching submodels for AAS {aas_id}: {e}")
        
        return submodels


class AASParser:    
    # Semantic ID patterns for entity type detection
    ENTITY_TYPE_PATTERNS = {
        'site': ['urn:idta:submodeltemplate:site:', 'site'],
        'device': [
            'urn:idta:submodeltemplate:device:', 
            'device',
            'https://admin-shell.io/idta/digitalnameplate',  # Digital Nameplate
            'admin-shell.io/idta/digitalnameplate'
        ],
        'resource': ['urn:idta:submodeltemplate:resource:', 'resource'],
        'supplier': ['urn:idta:submodeltemplate:supplier:', 'supplier'],
        'process': ['urn:idta:submodeltemplate:process:', 'process'],
        'product': ['urn:idta:submodeltemplate:product:', 'product'],
        'endproduct': ['urn:idta:submodeltemplate:product:end:', 'endproduct'],
        'intermediaryproduct': ['urn:idta:submodeltemplate:product:intermediary:', 'intermediaryproduct'],
        'processconfiguration': [
            'urn:idta:submodeltemplate:processconfiguration:', 
            'urn:idta:submodeltemplate:processconfig:', 
            'processconfiguration', 
            'processconfig'
        ],
        'logisticroute': ['urn:idta:submodeltemplate:logisticroute:', 'logisticroute'],
        'property': ['urn:idta:submodeltemplate:property:', 'property'],
        'sensor': ['urn:idta:submodeltemplate:sensor:', 'sensor'],
        'customerrequirement': ['urn:idta:submodeltemplate:customerrequirement:', 'customerrequirement']
    }
    
    # Detect entity type from semantic ID (which is unique per asset)
    @staticmethod
    def detect_entity_type(submodel: Dict) -> Optional[str]:
        semantic_id = submodel.get('semanticId', {})
        if 'keys' in semantic_id and len(semantic_id['keys']) > 0:
            semantic_value = semantic_id['keys'][0].get('value', '').lower()
            
            for entity_type, patterns in AASParser.ENTITY_TYPE_PATTERNS.items():
                for pattern in patterns:
                    if pattern in semantic_value:
                        return entity_type
        return None
    
    # Extract properties from submodel elements recursively for nested collections
    @staticmethod
    def extract_properties(submodel_elements: List[Dict]) -> Dict[str, Any]:
        properties = {}
        
        def extract_from_elements(elements: List[Dict], prefix: str = ''):
            for element in elements:
                model_type = element.get('modelType')
                id_short = element.get('idShort')
                
                if model_type == 'Property':
                    key = f"{prefix}{id_short}" if prefix else id_short
                    properties[key] = element.get('value')
                elif model_type == 'MultiLanguageProperty':
                    # Take first language value
                    values = element.get('value', [])
                    if values:
                        key = f"{prefix}{id_short}" if prefix else id_short
                        properties[key] = values[0].get('text')
                elif model_type == 'SubmodelElementCollection':
                    # Recursively extract from collections
                    nested_elements = element.get('value', [])
                    new_prefix = f"{prefix}{id_short}_" if prefix else f"{id_short}_"
                    extract_from_elements(nested_elements, new_prefix)
        
        extract_from_elements(submodel_elements)
        return properties
    
    # Extract reference elements (relationships)
    @staticmethod
    def extract_references(submodel_elements: List[Dict]) -> Dict[str, List[str]]:
        references = {}
        
        def extract_from_elements(elements: List[Dict], collection_name: str = None):
            for element in elements:
                model_type = element.get('modelType')
                id_short = element.get('idShort')
                
                if model_type == 'ReferenceElement':
                    ref_value = element.get('value', {})
                    if 'keys' in ref_value and len(ref_value['keys']) > 0:
                        ref_id = ref_value['keys'][0].get('value')
                        if ref_id:
                            # Use collection name if available, otherwise use idShort
                            key = collection_name if collection_name else id_short
                            if key not in references:
                                references[key] = []
                            references[key].append(ref_id)
                
                elif model_type == 'SubmodelElementCollection':
                    # Handle collections of references
                    collection_values = element.get('value', [])
                    
                    # Check if this collection contains references
                    has_references = any(
                        item.get('modelType') in ['ReferenceElement', 'SubmodelElementCollection']
                        for item in collection_values
                    )
                    
                    if has_references:
                        # Pass the collection name down
                        extract_from_elements(collection_values, id_short)
        
        extract_from_elements(submodel_elements)
        
        # Debug logging
        logger.debug(f"Extracted references: {references}")
        
        return references
    
    @staticmethod
    def parse_submodel(submodel: Dict) -> Dict[str, Any]:
        entity_type = AASParser.detect_entity_type(submodel)
        submodel_elements = submodel.get('submodelElements', [])
        
        parsed = {
            'id': submodel.get('id'),
            'idShort': submodel.get('idShort'),
            'entity_type': entity_type,
            'properties': AASParser.extract_properties(submodel_elements),
            'references': AASParser.extract_references(submodel_elements)
        }
        
        # Debug logging
        logger.debug(f"Parsed submodel {parsed['id']}: type={entity_type}, refs={list(parsed['references'].keys())}")
        
        return parsed


class KnowledgeGraphUpdater:
    
    def __init__(self, config: Neo4jConfig):
        self.driver = GraphDatabase.driver(
            config.URI, 
            auth=(config.USER, config.PASSWORD)
        )
        self.database = config.DATABASE
    
    def close(self):
        self.driver.close()
    
    def _execute_query(self, query: str, parameters: Dict = None):
        with self.driver.session(database=self.database) as session:
            try:
                result = session.run(query, parameters or {})
                return result.data()
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                logger.error(f"Query: {query}")
                logger.error(f"Parameters: {parameters}")
                raise
    
    def update_site(self, parsed_data: Dict):
        site_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        # Create/Update Site node
        query = """
        MERGE (s:Site {id: $site_id})
        SET s.name = $name,
            s.identifier = $identifier,
            s.address = $address,
            s.lastUpdated = datetime()
        RETURN s
        """
        
        params = {
            'site_id': site_id,
            'name': props.get('SiteName', ''),
            'identifier': props.get('SiteIdentifier', ''),
            'address': props.get('LocationAddress', '')
            # 'designation': props.get('LocationAspectDesignation', '')
        }
        
        self._execute_query(query, params)
        # # logger.info(f"Updated Site: {site_id}")
        
        # Create relationships to Devices
        device_keys = ['Devices', 'HasDevice', 'InstalledDevices']
        device_refs = []
        for key in device_keys:
            if key in refs:
                device_refs.extend(refs[key])
        
        for device_id in device_refs:
            rel_query = """
            MATCH (s:Site {id: $site_id})
            MERGE (d:Device {id: $device_id})
            MERGE (s)-[:HAS_DEVICE]->(d)
            """
            self._execute_query(rel_query, {'site_id': site_id, 'device_id': device_id})
            # # logger.info(f"Created HAS_DEVICE: {site_id} -> {device_id}")
        
        # Create relationships to Processes
        process_keys = ['PerformedProcesses', 'Processes']
        process_refs = []
        for key in process_keys:
            if key in refs:
                process_refs.extend(refs[key])
        
        for process_id in process_refs:
            rel_query = """
            MATCH (s:Site {id: $site_id})
            MERGE (p:Process {id: $process_id})
            MERGE (p)-[:PERFORMED_AT]->(s)
            """
            self._execute_query(rel_query, {'site_id': site_id, 'process_id': process_id})
            # # logger.info(f"Created PERFORMED_AT: {process_id} -> {site_id}")
        
        # Create relationships to LogisticRoutes
        route_keys = ['LogisticConnections', 'Routes']
        route_refs = []
        for key in route_keys:
            if key in refs:
                route_refs.extend(refs[key])
        
        for route_id in route_refs:
            rel_query = """
            MATCH (s:Site {id: $site_id})
            MERGE (lr:LogisticRoute {id: $route_id})
            MERGE (lr)-[:FROM_SITE]->(s)
            """
            self._execute_query(rel_query, {'site_id': site_id, 'route_id': route_id})
    
    def update_device(self, parsed_data: Dict):
        device_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (d:Device {id: $device_id})
        SET d.name = $name,
            d.identifier = $identifier,
            d.type = $type,
            d.manufacturer = $manufacturer,
            d.serialNumber = $serial_number,
            d.maxPower = $max_power,
            d.weight = $weight,
            d.weightUnit = $weight_unit,
            d.lastUpdated = datetime()
        RETURN d
        """
        
        # Handle both custom template and Digital Nameplate structure
        params = {
            'device_id': device_id,
            'name': props.get('DeviceName', 
                             props.get('Nameplate_ManufacturerProductDesignation', 
                                     props.get('Name', ''))),
            'identifier': props.get('DeviceIdentifier', 
                                  props.get('Nameplate_SerialNumber', '')),
            'type': props.get('DeviceType', ''),
            'manufacturer': props.get('Manufacturer', 
                                    props.get('Nameplate_ManufacturerName', '')),
            'serial_number': props.get('Nameplate_SerialNumber', ''),
            'max_power': props.get('TechnicalData_MaxPower', ''),
            'weight': props.get('TechnicalData_Weight', ''),
            'weight_unit': props.get('TechnicalData_WeightUnit', '')
        }
        
        self._execute_query(query, params)
        # # logger.info(f"Updated Device: {device_id}")
        
        # Link to Site (reverse relationship)
        site_keys = ['LocationSite', 'Site', 'InstalledAtSite']
        site_refs = []
        for key in site_keys:
            if key in refs:
                site_refs.extend(refs[key])
        
        if site_refs:
            for site_id in site_refs:
                # # logger.info(f"Creating HAS_DEVICE relationship (from Site): {site_id} -> {device_id}")
                rel_query = """
                MATCH (d:Device {id: $device_id})
                MERGE (s:Site {id: $site_id})
                MERGE (s)-[:HAS_DEVICE]->(d)
                """
                self._execute_query(rel_query, {'device_id': device_id, 'site_id': site_id})
                # logger.info(f"Successfully created HAS_DEVICE: {site_id} -> {device_id}")
        
        # Link to Properties
        property_keys = ['Properties', 'HasProperty', 'DeviceProperties']
        property_refs = []
        for key in property_keys:
            if key in refs:
                property_refs.extend(refs[key])
        
        if property_refs:
            # logger.info(f"Found {len(property_refs)} property references for {device_id}")
            for prop_id in property_refs:
                # logger.info(f"Creating HAS_PROPERTY relationship: {device_id} -> {prop_id}")
                rel_query = """
                MATCH (d:Device {id: $device_id})
                MERGE (p:Property {id: $prop_id})
                MERGE (d)-[:HAS_PROPERTY]->(p)
                """
                try:
                    self._execute_query(rel_query, {'device_id': device_id, 'prop_id': prop_id})
                    # logger.info(f"Successfully created HAS_PROPERTY: {device_id} -> {prop_id}")
                except Exception as e:
                    logger.error(f"Failed to create HAS_PROPERTY: {e}")
    
    def update_resource(self, parsed_data: Dict):
        resource_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (r:Resource {id: $resource_id})
        SET r.name = $name,
            r.identifier = $identifier,
            r.type = $type,
            r.specification = $specification,
            r.availableQuantity = $available_quantity,
            r.unitOfMeasure = $unit_of_measure,
            r.lastUpdated = datetime()
        RETURN r
        """
        
        params = {
            'resource_id': resource_id,
            'name': props.get('ResourceName', props.get('Name', '')),
            'identifier': props.get('ResourceIdentifier', ''),
            'type': props.get('ResourceType', ''),
            'specification': props.get('Specification', ''),
            'available_quantity': props.get('AvailableQuantity', ''),
            'unit_of_measure': props.get('UnitOfMeasure', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated Resource: {resource_id}")
        
        # Link to Supplier - PROVIDED_BY
        supplier_keys = ['Supplier', 'ProvidedBySupplier', 'ProvidedBy', 'SupplierReference']
        supplier_refs = []
        for key in supplier_keys:
            if key in refs:
                supplier_refs.extend(refs[key])
        
        if supplier_refs:
            # logger.info(f"Found {len(supplier_refs)} supplier references for {resource_id}")
            for supplier_id in supplier_refs:
                # logger.info(f"Creating PROVIDED_BY relationship: {resource_id} -> {supplier_id}")
                rel_query = """
                MATCH (r:Resource {id: $resource_id})
                MERGE (s:Supplier {id: $supplier_id})
                MERGE (r)-[:PROVIDED_BY]->(s)
                """
                try:
                    self._execute_query(rel_query, {'resource_id': resource_id, 'supplier_id': supplier_id})
                    # logger.info(f"Successfully created PROVIDED_BY: {resource_id} -> {supplier_id}")
                except Exception as e:
                    logger.error(f"Failed to create PROVIDED_BY: {e}")
        
        # Link to Properties - HAS_PROPERTY
        property_keys = ['Properties', 'TechnicalProperties', 'HasProperty', 'ResourceProperties']
        property_refs = []
        for key in property_keys:
            if key in refs:
                property_refs.extend(refs[key])
        
        if property_refs:
            # logger.info(f"Found {len(property_refs)} property references for {resource_id}")
            for prop_id in property_refs:
                # logger.info(f"Creating HAS_PROPERTY relationship: {resource_id} -> {prop_id}")
                rel_query = """
                MATCH (r:Resource {id: $resource_id})
                MERGE (p:Property {id: $prop_id})
                MERGE (r)-[:HAS_PROPERTY]->(p)
                """
                try:
                    self._execute_query(rel_query, {'resource_id': resource_id, 'prop_id': prop_id})
                    # logger.info(f"Successfully created HAS_PROPERTY: {resource_id} -> {prop_id}")
                except Exception as e:
                    logger.error(f"Failed to create HAS_PROPERTY: {e}")
        
        # Handle UsedInConfigurations - USES_RESOURCE (reverse)
        config_keys = ['UsedInConfigurations', 'UsedByConfigurations', 'Configurations']
        config_refs = []
        for key in config_keys:
            if key in refs:
                config_refs.extend(refs[key])
        
        if config_refs:
            # logger.info(f"Found {len(config_refs)} process configuration references for {resource_id}")
            for config_id in config_refs:
                # logger.info(f"Creating USES_RESOURCE relationship: {config_id} -> {resource_id}")
                rel_query = """
                MATCH (r:Resource {id: $resource_id})
                MERGE (pc:ProcessConfiguration {id: $config_id})
                MERGE (pc)-[:USES_RESOURCE]->(r)
                """
                try:
                    self._execute_query(rel_query, {'resource_id': resource_id, 'config_id': config_id})
                    # logger.info(f"Successfully created USES_RESOURCE: {config_id} -> {resource_id}")
                except Exception as e:
                    logger.error(f"Failed to create USES_RESOURCE: {e}")
    
    def update_supplier(self, parsed_data: Dict):
        supplier_id = parsed_data['id']
        props = parsed_data['properties']
        
        query = """
        MERGE (s:Supplier {id: $supplier_id})
        SET s.name = $name,
            s.identifier = $identifier,
            s.address = $address,
            s.contact = $contact,
            s.lastUpdated = datetime()
        RETURN s
        """
        
        params = {
            'supplier_id': supplier_id,
            'name': props.get('SupplierName', props.get('Name', '')),
            'identifier': props.get('SupplierIdentifier', ''),
            'address': props.get('Address', ''),
            'contact': props.get('ContactInformation', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated Supplier: {supplier_id}")
    
    def update_process(self, parsed_data: Dict):
        process_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (p:Process {id: $process_id})
        SET p.name = $name,
            p.identifier = $identifier,
            p.type = $type,
            p.description = $description,
            p.designation = $designation,
            p.lastUpdated = datetime()
        RETURN p
        """
        
        params = {
            'process_id': process_id,
            'name': props.get('ProcessName', props.get('Name', '')),
            'identifier': props.get('ProcessIdentifier', ''),
            'type': props.get('ProcessType', ''),
            'description': props.get('ProcessDescription', props.get('Description', '')),
            'designation': props.get('FunctionAspectDesignation', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated Process: {process_id}")
        
        # Link to Site - PERFORMED_AT
        site_keys = ['PerformedAtSite', 'Site', 'LocationSite']
        site_refs = []
        for key in site_keys:
            if key in refs:
                site_refs.extend(refs[key])
        
        if site_refs:
            # logger.info(f"Found {len(site_refs)} site references for {process_id}")
            for site_id in site_refs:
                # logger.info(f"Creating PERFORMED_AT relationship: {process_id} -> {site_id}")
                rel_query = """
                MATCH (p:Process {id: $process_id})
                MERGE (s:Site {id: $site_id})
                MERGE (p)-[:PERFORMED_AT]->(s)
                """
                try:
                    self._execute_query(rel_query, {'process_id': process_id, 'site_id': site_id})
                    # logger.info(f"Successfully created PERFORMED_AT: {process_id} -> {site_id}")
                except Exception as e:
                    logger.error(f"Failed to create PERFORMED_AT: {e}")
        
        # Handle ProcessConfigurations - REQUIRES_PROCESS (reverse)
        config_keys = ['ProcessConfigurations', 'Configurations', 'UsedInConfigurations']
        config_refs = []
        for key in config_keys:
            if key in refs:
                config_refs.extend(refs[key])
        
        if config_refs:
            # logger.info(f"Found {len(config_refs)} process configuration references for {process_id}")
            for config_id in config_refs:
                # logger.info(f"Creating REQUIRES_PROCESS relationship: {config_id} -> {process_id}")
                rel_query = """
                MATCH (p:Process {id: $process_id})
                MERGE (pc:ProcessConfiguration {id: $config_id})
                MERGE (pc)-[:REQUIRES_PROCESS]->(p)
                """
                try:
                    self._execute_query(rel_query, {'process_id': process_id, 'config_id': config_id})
                    # logger.info(f"Successfully created REQUIRES_PROCESS: {config_id} -> {process_id}")
                except Exception as e:
                    logger.error(f"Failed to create REQUIRES_PROCESS: {e}")
    
    def update_process_configuration(self, parsed_data: Dict):
        config_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (pc:ProcessConfiguration {id: $config_id})
        SET pc.name = $name,
            pc.identifier = $identifier,
            pc.lastUpdated = datetime()
        RETURN pc
        """
        
        params = {
            'config_id': config_id,
            'name': props.get('ConfigurationName', props.get('Name', '')),
            'identifier': props.get('ConfigurationIdentifier', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated ProcessConfiguration: {config_id}")
        
        # Link to Process - REQUIRES_PROCESS
        process_keys = ['Process', 'RequiredProcess', 'RequiresProcess']
        process_refs = []
        for key in process_keys:
            if key in refs:
                process_refs.extend(refs[key])
        
        for process_id in process_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (p:Process {id: $process_id})
            MERGE (pc)-[:REQUIRES_PROCESS]->(p)
            """
            self.execute_query(rel_query, {'config_id': config_id, 'process_id': process_id})
            # logger.info(f"Created REQUIRES_PROCESS: {config_id} -> {process_id}")
        
        # Link to Resources - USES_RESOURCE
        resource_keys = ['Resources', 'UsedResources', 'UsesResource']
        resource_refs = []
        for key in resource_keys:
            if key in refs:
                resource_refs.extend(refs[key])
        
        for resource_id in resource_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (r:Resource {id: $resource_id})
            MERGE (pc)-[:USES_RESOURCE]->(r)
            """
            self._execute_query(rel_query, {'config_id': config_id, 'resource_id': resource_id})
            # logger.info(f"Created USES_RESOURCE: {config_id} -> {resource_id}")
        
        # Link to Output EndProduct - OUTPUT_END_PRODUCT
        end_product_keys = ['OutputEndProduct', 'OutputProduct', 'ProducesEndProduct']
        end_product_refs = []
        for key in end_product_keys:
            if key in refs:
                end_product_refs.extend(refs[key])
        
        for product_id in end_product_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (ep:EndProduct {id: $product_id})
            MERGE (pc)-[:OUTPUT_END_PRODUCT]->(ep)
            """
            self._execute_query(rel_query, {'config_id': config_id, 'product_id': product_id})
            # logger.info(f"Created OUTPUT_END_PRODUCT: {config_id} -> {product_id}")
        
        # Link to Output IntermediaryProduct - OUTPUT_PRODUCT
        intermediary_keys = ['OutputIntermediaryProduct', 'ProducesIntermediaryProduct']
        intermediary_refs = []
        for key in intermediary_keys:
            if key in refs:
                intermediary_refs.extend(refs[key])
        
        for product_id in intermediary_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (ip:IntermediaryProduct {id: $product_id})
            MERGE (pc)-[:OUTPUT_PRODUCT]->(ip)
            """
            self._execute_query(rel_query, {'config_id': config_id, 'product_id': product_id})
            # logger.info(f"Created OUTPUT_PRODUCT: {config_id} -> {product_id}")
        
        # Link to Input Products - INPUT_PRODUCT (reverse)
        input_keys = ['InputProduct', 'InputProducts', 'RequiredProducts']
        input_refs = []
        for key in input_keys:
            if key in refs:
                input_refs.extend(refs[key])
        
        for product_id in input_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (ip:IntermediaryProduct {id: $product_id})
            MERGE (ip)-[:INPUT_PRODUCT]->(pc)
            """
            self._execute_query(rel_query, {'config_id': config_id, 'product_id': product_id})
            # logger.info(f"Created INPUT_PRODUCT: {product_id} -> {config_id}")
        
        # Link to dependent configurations - DEPENDS_ON
        dep_keys = ['DependsOn', 'DependentConfigurations', 'Dependencies']
        dep_refs = []
        for key in dep_keys:
            if key in refs:
                dep_refs.extend(refs[key])
        
        for dep_config_id in dep_refs:
            rel_query = """
            MATCH (pc:ProcessConfiguration {id: $config_id})
            MERGE (pc2:ProcessConfiguration {id: $dep_config_id})
            MERGE (pc)-[:DEPENDS_ON]->(pc2)
            """
            self._execute_query(rel_query, {'config_id': config_id, 'dep_config_id': dep_config_id})
            # logger.info(f"Created DEPENDS_ON: {config_id} -> {dep_config_id}")
    
    def update_product(self, parsed_data: Dict):
        product_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        entity_type = parsed_data['entity_type']
        
        # Determine if EndProduct or IntermediaryProduct
        node_label = "EndProduct" if entity_type == 'endproduct' else "IntermediaryProduct"
        
        query = f"""
        MERGE (p:{node_label} {{id: $product_id}})
        SET p.name = $name,
            p.identifier = $identifier,
            p.description = $description,
            p.specification = $specification,
            p.designation = $designation,
            p.weight = $weight,
            p.weightUnit = $weight_unit,
            p.lastUpdated = datetime()
        RETURN p
        """
        
        params = {
            'product_id': product_id,
            'name': props.get('ProductName', props.get('Name', '')),
            'identifier': props.get('ProductIdentifier', ''),
            'description': props.get('Description', ''),
            'specification': props.get('Specification', ''),
            'designation': props.get('ProductAspectDesignation', ''),
            'weight': props.get('ProductWeight', ''),
            'weight_unit': props.get('WeightUnit', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated {node_label}: {product_id}")
        
        # SATISFIES_REQUIREMENT - Check all possible key names
        requirement_keys = ['CustomerRequirement', 'SatisfiesRequirement', 'SatisfiedRequirements', 'Requirements']
        req_refs = []
        for key in requirement_keys:
            if key in refs:
                req_refs.extend(refs[key])
        
        if req_refs:
            # logger.info(f"Found {len(req_refs)} customer requirement references for {product_id}")
            for req_id in req_refs:
                # logger.info(f"Creating SATISFIES_REQUIREMENT relationship: {product_id} -> {req_id}")
                rel_query = f"""
                MATCH (p:{node_label} {{id: $product_id}})
                MERGE (cr:CustomerRequirement {{id: $req_id}})
                MERGE (p)-[:SATISFIES_REQUIREMENT]->(cr)
                """
                try:
                    self._execute_query(rel_query, {'product_id': product_id, 'req_id': req_id})
                    # logger.info(f"Successfully created SATISFIES_REQUIREMENT: {product_id} -> {req_id}")
                except Exception as e:
                    logger.error(f"Failed to create SATISFIES_REQUIREMENT: {e}")
        else:
            logger.debug(f"No customer requirement references found for {product_id}")
        
        # HAS_COMPONENT - Link EndProduct to IntermediaryProduct components (BillOfMaterial)
        if entity_type == 'endproduct':
            component_keys = ['Components', 'BillOfMaterial', 'HasComponent']
            component_refs = []
            for key in component_keys:
                if key in refs:
                    component_refs.extend(refs[key])
            
            if component_refs:
                # logger.info(f"Found {len(component_refs)} component references for {product_id}")
                for component_id in component_refs:
                    mandatory = props.get(f'{component_id}_mandatory', 'true').lower() == 'true'
                    rel_query = """
                    MATCH (ep:EndProduct {id: $product_id})
                    MERGE (ip:IntermediaryProduct {id: $component_id})
                    MERGE (ep)-[:HAS_COMPONENT {mandatory: $mandatory}]->(ip)
                    """
                    self._execute_query(rel_query, {
                        'product_id': product_id, 
                        'component_id': component_id,
                        'mandatory': mandatory
                    })
                    # logger.info(f"Created HAS_COMPONENT: {product_id} -> {component_id}")
        
        # Link to ProcessConfiguration (ProducedByConfiguration)
        config_keys = ['ProducedByConfiguration', 'ProcessConfiguration', 'Configuration', 'ProducedBy']
        config_refs = []
        for key in config_keys:
            if key in refs:
                config_refs.extend(refs[key])
        
        for config_id in config_refs:
            if entity_type == 'endproduct':
                rel_query = f"""
                MATCH (p:{node_label} {{id: $product_id}})
                MERGE (pc:ProcessConfiguration {{id: $config_id}})
                MERGE (pc)-[:OUTPUT_END_PRODUCT]->(p)
                """
            else:
                rel_query = f"""
                MATCH (p:{node_label} {{id: $product_id}})
                MERGE (pc:ProcessConfiguration {{id: $config_id}})
                MERGE (pc)-[:OUTPUT_PRODUCT]->(p)
                """
            self._execute_query(rel_query, {'product_id': product_id, 'config_id': config_id})
            # logger.info(f"Created output relationship: {config_id} -> {product_id}")
        
        # INPUT_PRODUCT - IntermediaryProduct to ProcessConfiguration
        if entity_type == 'intermediaryproduct':
            input_config_keys = ['UsedInConfiguration', 'InputToConfiguration']
            input_config_refs = []
            for key in input_config_keys:
                if key in refs:
                    input_config_refs.extend(refs[key])
            
            for config_id in input_config_refs:
                rel_query = """
                MATCH (ip:IntermediaryProduct {id: $product_id})
                MERGE (pc:ProcessConfiguration {id: $config_id})
                MERGE (ip)-[:INPUT_PRODUCT]->(pc)
                """
                self._execute_query(rel_query, {'product_id': product_id, 'config_id': config_id})
                # logger.info(f"Created INPUT_PRODUCT: {product_id} -> {config_id}")
    
    def update_logistic_route(self, parsed_data: Dict):
        route_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (lr:LogisticRoute {id: $route_id})
        SET lr.name = $name,
            lr.identifier = $identifier,
            lr.transportMode = $transport_mode,
            lr.distance = $distance,
            lr.estimatedTransitTime = $estimated_transit_time,
            lr.cost = $cost,
            lr.currency = $currency,
            lr.carrier = $carrier,
            lr.routeStatus = $route_status,
            lr.originLocationType = $origin_location_type,
            lr.destinationLocationType = $destination_location_type,
            lr.lastUpdated = datetime()
        RETURN lr
        """
        
        params = {
            'route_id': route_id,
            'name': props.get('RouteName', props.get('Name', '')),
            'identifier': props.get('RouteIdentifier', ''),
            'transport_mode': props.get('TransportMode', ''),
            'distance': props.get('Distance', ''),
            'estimated_transit_time': props.get('EstimatedTransitTime', ''),
            'cost': props.get('Cost', ''),
            'currency': props.get('Currency', ''),
            'carrier': props.get('Carrier', ''),
            'route_status': props.get('RouteStatus', ''),
            'origin_location_type': props.get('OriginLocation_LocationType', ''),
            'destination_location_type': props.get('DestinationLocation_LocationType', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated LogisticRoute: {route_id}")
        
        # Link FROM_SUPPLIER (from OriginLocation collection)
        from_supplier_keys = ['OriginLocation', 'FromSupplier', 'SourceSupplier']
        from_supplier_refs = []
        for key in from_supplier_keys:
            if key in refs:
                from_supplier_refs.extend(refs[key])
        
        if from_supplier_refs:
            # logger.info(f"Found {len(from_supplier_refs)} FROM_SUPPLIER references for {route_id}")
            for supplier_id in from_supplier_refs:
                # logger.info(f"Creating FROM_SUPPLIER relationship: {route_id} -> {supplier_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (s:Supplier {id: $supplier_id})
                MERGE (lr)-[:FROM_SUPPLIER]->(s)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'supplier_id': supplier_id})
                    # logger.info(f"Successfully created FROM_SUPPLIER: {route_id} -> {supplier_id}")
                except Exception as e:
                    logger.error(f"Failed to create FROM_SUPPLIER: {e}")
        
        # Link TO_SUPPLIER (from DestinationLocation collection if LocationType is Supplier)
        to_supplier_keys = ['DestinationLocation', 'ToSupplier', 'TargetSupplier']
        to_supplier_refs = []
        
        # Check if destination is a supplier
        if props.get('DestinationLocation_LocationType', '').lower() == 'supplier':
            for key in to_supplier_keys:
                if key in refs:
                    to_supplier_refs.extend(refs[key])
        
        if to_supplier_refs:
            # logger.info(f"Found {len(to_supplier_refs)} TO_SUPPLIER references for {route_id}")
            for supplier_id in to_supplier_refs:
                # logger.info(f"Creating TO_SUPPLIER relationship: {route_id} -> {supplier_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (s:Supplier {id: $supplier_id})
                MERGE (lr)-[:TO_SUPPLIER]->(s)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'supplier_id': supplier_id})
                    # logger.info(f"Successfully created TO_SUPPLIER: {route_id} -> {supplier_id}")
                except Exception as e:
                    logger.error(f"Failed to create TO_SUPPLIER: {e}")
        
        # Link FROM_SITE (from OriginLocation collection if LocationType is Site)
        from_site_keys = ['OriginLocation', 'FromSite', 'SourceSite']
        from_site_refs = []
        
        # Check if origin is a site
        if props.get('OriginLocation_LocationType', '').lower() == 'site':
            for key in from_site_keys:
                if key in refs:
                    from_site_refs.extend(refs[key])
        
        if from_site_refs:
            # logger.info(f"Found {len(from_site_refs)} FROM_SITE references for {route_id}")
            for site_id in from_site_refs:
                # logger.info(f"Creating FROM_SITE relationship: {route_id} -> {site_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (s:Site {id: $site_id})
                MERGE (lr)-[:FROM_SITE]->(s)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'site_id': site_id})
                    # logger.info(f"Successfully created FROM_SITE: {route_id} -> {site_id}")
                except Exception as e:
                    logger.error(f"Failed to create FROM_SITE: {e}")
        
        # Link TO_SITE (from DestinationLocation collection)
        to_site_keys = ['DestinationLocation', 'ToSite', 'TargetSite']
        to_site_refs = []
        for key in to_site_keys:
            if key in refs:
                to_site_refs.extend(refs[key])
        
        if to_site_refs:
            # logger.info(f"Found {len(to_site_refs)} TO_SITE references for {route_id}")
            for site_id in to_site_refs:
                # logger.info(f"Creating TO_SITE relationship: {route_id} -> {site_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (s:Site {id: $site_id})
                MERGE (lr)-[:TO_SITE]->(s)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'site_id': site_id})
                    # logger.info(f"Successfully created TO_SITE: {route_id} -> {site_id}")
                except Exception as e:
                    logger.error(f"Failed to create TO_SITE: {e}")
        
        # Link TRANSPORTS (Resources being transported)
        transported_keys = ['TransportedMaterials', 'TransportedResources', 'Cargo']
        transported_refs = []
        for key in transported_keys:
            if key in refs:
                transported_refs.extend(refs[key])
        
        if transported_refs:
            # logger.info(f"Found {len(transported_refs)} transported resource references for {route_id}")
            for resource_id in transported_refs:
                # logger.info(f"Creating TRANSPORTS relationship: {route_id} -> {resource_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (r:Resource {id: $resource_id})
                MERGE (lr)-[:TRANSPORTS]->(r)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'resource_id': resource_id})
                    # logger.info(f"Successfully created TRANSPORTS: {route_id} -> {resource_id}")
                except Exception as e:
                    logger.error(f"Failed to create TRANSPORTS: {e}")
        
        # Link HAS_PROPERTY (Technical Properties)
        property_keys = ['TechnicalProperties', 'Properties', 'RouteProperties']
        property_refs = []
        for key in property_keys:
            if key in refs:
                property_refs.extend(refs[key])
        
        if property_refs:
            # logger.info(f"Found {len(property_refs)} property references for {route_id}")
            for prop_id in property_refs:
                # logger.info(f"Creating HAS_PROPERTY relationship: {route_id} -> {prop_id}")
                rel_query = """
                MATCH (lr:LogisticRoute {id: $route_id})
                MERGE (p:Property {id: $prop_id})
                MERGE (lr)-[:HAS_PROPERTY]->(p)
                """
                try:
                    self._execute_query(rel_query, {'route_id': route_id, 'prop_id': prop_id})
                    # logger.info(f"Successfully created HAS_PROPERTY: {route_id} -> {prop_id}")
                except Exception as e:
                    logger.error(f"Failed to create HAS_PROPERTY: {e}")

    
    def update_property(self, parsed_data: Dict):
        property_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        # Extract the dynamic property name and value
        # Look for property elements that are not standard metadata
        standard_keys = {
            'PropertyIdentifier', 'PropertyValue', 'DataType', 'Unit', 
            'MinValue', 'MaxValue', 'NominalValue', 'Description'
        }
        
        dynamic_property_name = None
        dynamic_property_value = None
        
        # Find the dynamic property (like "temperature")
        for key, value in props.items():
            if key not in standard_keys and not key.startswith('ObservingSensors'):
                dynamic_property_name = key
                dynamic_property_value = value
                break
        
        query = """
        MERGE (p:Property {id: $property_id})
        SET p.name = $name,
            p.identifier = $identifier,
            p.propertyName = $property_name,
            p.currentValue = $current_value,
            p.propertyValue = $property_value,
            p.dataType = $data_type,
            p.unit = $unit,
            p.minValue = $min_value,
            p.maxValue = $max_value,
            p.nominalValue = $nominal_value,
            p.description = $description,
            p.lastUpdated = datetime()
        RETURN p
        """
        
        params = {
            'property_id': property_id,
            'name': dynamic_property_name or props.get('PropertyName', 'Unknown'),
            'identifier': props.get('PropertyIdentifier', ''),
            'property_name': dynamic_property_name or '',
            'current_value': dynamic_property_value or props.get('PropertyValue', ''),
            'property_value': props.get('PropertyValue', ''),
            'data_type': props.get('DataType', ''),
            'unit': props.get('Unit', ''),
            'min_value': props.get('MinValue', ''),
            'max_value': props.get('MaxValue', ''),
            'nominal_value': props.get('NominalValue', ''),
            'description': props.get('Description', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated Property: {property_id} (name={dynamic_property_name}, value={dynamic_property_value})")
        
        # Link to Sensors - OBSERVES (reverse relationship)
        # Handle both ObservedBy and ObservingSensors
        sensor_keys = ['ObservedBy', 'ObservingSensors', 'Sensors', 'MeasuredBy']
        sensor_refs = []
        for key in sensor_keys:
            if key in refs:
                sensor_refs.extend(refs[key])
        
        if sensor_refs:
            # logger.info(f"Found {len(sensor_refs)} sensor references for {property_id}")
            for sensor_id in sensor_refs:
                # logger.info(f"Creating OBSERVES relationship: {sensor_id} -> {property_id}")
                rel_query = """
                MATCH (p:Property {id: $property_id})
                MERGE (s:Sensor {id: $sensor_id})
                MERGE (s)-[:OBSERVES]->(p)
                """
                try:
                    self._execute_query(rel_query, {'property_id': property_id, 'sensor_id': sensor_id})
                    # logger.info(f"Successfully created OBSERVES: {sensor_id} -> {property_id}")
                except Exception as e:
                    logger.error(f"Failed to create OBSERVES: {e}")
        else:
            logger.debug(f"No sensor references found for {property_id}")

    
    def update_sensor(self, parsed_data: Dict):
        sensor_id = parsed_data['id']
        props = parsed_data['properties']
        refs = parsed_data['references']
        
        query = """
        MERGE (s:Sensor {id: $sensor_id})
        SET s.name = $name,
            s.identifier = $identifier,
            s.type = $type,
            s.manufacturer = $manufacturer,
            s.lastUpdated = datetime()
        RETURN s
        """
        
        params = {
            'sensor_id': sensor_id,
            'name': props.get('SensorName', props.get('Name', '')),
            'identifier': props.get('SensorIdentifier', ''),
            'type': props.get('SensorType', ''),
            'manufacturer': props.get('Manufacturer', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated Sensor: {sensor_id}")
        
        # Link to Properties - OBSERVES
        property_keys = ['ObservedProperties', 'Properties', 'Measures']
        property_refs = []
        for key in property_keys:
            if key in refs:
                property_refs.extend(refs[key])
        
        for prop_id in property_refs:
            rel_query = """
            MATCH (s:Sensor {id: $sensor_id})
            MERGE (p:Property {id: $prop_id})
            MERGE (s)-[:OBSERVES]->(p)
            """
            self._execute_query(rel_query, {'sensor_id': sensor_id, 'prop_id': prop_id})
            # logger.info(f"Created OBSERVES: {sensor_id} -> {prop_id}")
    
    def update_customer_requirement(self, parsed_data: Dict):
        requirement_id = parsed_data['id']
        props = parsed_data['properties']
        
        query = """
        MERGE (cr:CustomerRequirement {id: $requirement_id})
        SET cr.name = $name,
            cr.identifier = $identifier,
            cr.description = $description,
            cr.priority = $priority,
            cr.lastUpdated = datetime()
        RETURN cr
        """
        
        params = {
            'requirement_id': requirement_id,
            'name': props.get('RequirementName', props.get('Name', '')),
            'identifier': props.get('RequirementIdentifier', ''),
            'description': props.get('Description', ''),
            'priority': props.get('Priority', '')
        }
        
        self._execute_query(query, params)
        # logger.info(f"Updated CustomerRequirement: {requirement_id}")
    
    def update_entity(self, parsed_data: Dict):
        entity_type = parsed_data.get('entity_type')
        
        if not entity_type:
            logger.warning(f"Unknown entity type for {parsed_data.get('id')}")
            return
        
        update_methods = {
            'site': self.update_site,
            'device': self.update_device,
            'resource': self.update_resource,
            'supplier': self.update_supplier,
            'process': self.update_process,
            'processconfiguration': self.update_process_configuration,
            'endproduct': self.update_product,
            'intermediaryproduct': self.update_product,
            'product': self.update_product,
            'logisticroute': self.update_logistic_route,
            'property': self.update_property,
            'sensor': self.update_sensor,
            'customerrequirement': self.update_customer_requirement
        }
        
        update_method = update_methods.get(entity_type)
        if update_method:
            try:
                update_method(parsed_data)
            except Exception as e:
                logger.error(f"Error updating {entity_type} {parsed_data.get('id')}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.warning(f"No update method for entity type: {entity_type}")


class AAStoKGSynchronizer:
    
    def __init__(self, basyx_config: BaSyxConfig, neo4j_config: Neo4jConfig):
        self.basyx_client = BaSyxClient(basyx_config)
        self.kg_updater = KnowledgeGraphUpdater(neo4j_config)
        self.parser = AASParser()
        self.processed_assets = {}  # Track processed assets and their timestamps
    
    def sync_single_aas(self, aas_id: str):
        try:
            # logger.info(f"Syncing AAS: {aas_id}")
            
            # Fetch AAS
            aas = self.basyx_client.get_aas_by_id(aas_id)
            if not aas:
                logger.warning(f"Could not fetch AAS: {aas_id}")
                return
            
            # Fetch all submodels
            submodels = self.basyx_client.get_all_submodels_for_aas(aas_id)
            
            if not submodels:
                logger.warning(f"No submodels found for AAS: {aas_id}")
                return
            
            # Parse and update each submodel
            for submodel in submodels:
                try:
                    parsed_data = self.parser.parse_submodel(submodel)
                    
                    if parsed_data['entity_type']:
                        self.kg_updater.update_entity(parsed_data)
                        
                        # Track processed asset
                        self.processed_assets[submodel['id']] = {
                            'aas_id': aas_id,
                            'timestamp': datetime.now().isoformat(),
                            'entity_type': parsed_data['entity_type']
                        }
                    else:
                        logger.warning(f"Could not determine entity type for submodel: {submodel.get('id')}")
                
                except Exception as e:
                    logger.error(f"Error processing submodel {submodel.get('id')}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    continue
            
            # logger.info(f"Successfully synced AAS: {aas_id}")
            
        except Exception as e:
            logger.error(f"Error syncing AAS {aas_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def sync_all_aas(self):
        try:
            # logger.info("Starting full synchronization...")
            
            # Get all AAS descriptors
            descriptors = self.basyx_client.get_all_aas_descriptors()
            
            if not descriptors:
                # logger.info("No AAS descriptors found in registry")
                return
            
            # logger.info(f"Found {len(descriptors)} AAS descriptors")
            
            # Process each AAS
            for descriptor in descriptors:
                aas_id = descriptor.get('id')
                if aas_id:
                    self.sync_single_aas(aas_id)
            
            # logger.info(f"Synchronization complete. Processed {len(self.processed_assets)} assets.")
            
        except Exception as e:
            logger.error(f"Error during synchronization: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def run_continuous_sync(self, poll_interval: int = None):
        interval = poll_interval or BaSyxConfig.POLL_INTERVAL
        
        # logger.info(f"Starting continuous synchronization (polling every {interval} seconds)")
        
        try:
            while True:
                self.sync_all_aas()
                # logger.info(f"Waiting {interval} seconds before next sync...")
                time.sleep(interval)
        
        # except KeyboardInterrupt:
        #     # logger.info("Synchronization stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in synchronization loop: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self.cleanup()
    
    def cleanup(self):
        # logger.info("Cleaning up resources...")
        self.kg_updater.close()
        # logger.info("Cleanup complete")


# Indexes in Neo4j for better performance
def create_indexes(neo4j_config: Neo4jConfig):
    updater = KnowledgeGraphUpdater(neo4j_config)
    
    indexes = [
        "CREATE INDEX site_id IF NOT EXISTS FOR (s:Site) ON (s.id)",
        "CREATE INDEX device_id IF NOT EXISTS FOR (d:Device) ON (d.id)",
        "CREATE INDEX resource_id IF NOT EXISTS FOR (r:Resource) ON (r.id)",
        "CREATE INDEX supplier_id IF NOT EXISTS FOR (s:Supplier) ON (s.id)",
        "CREATE INDEX process_id IF NOT EXISTS FOR (p:Process) ON (p.id)",
        "CREATE INDEX process_config_id IF NOT EXISTS FOR (pc:ProcessConfiguration) ON (pc.id)",
        "CREATE INDEX end_product_id IF NOT EXISTS FOR (ep:EndProduct) ON (ep.id)",
        "CREATE INDEX intermediary_product_id IF NOT EXISTS FOR (ip:IntermediaryProduct) ON (ip.id)",
        "CREATE INDEX logistic_route_id IF NOT EXISTS FOR (lr:LogisticRoute) ON (lr.id)",
        "CREATE INDEX property_id IF NOT EXISTS FOR (p:Property) ON (p.id)",
        "CREATE INDEX sensor_id IF NOT EXISTS FOR (s:Sensor) ON (s.id)",
        "CREATE INDEX customer_req_id IF NOT EXISTS FOR (cr:CustomerRequirement) ON (cr.id)"
    ]
    
    # logger.info("Creating Neo4j indexes...")
    for index_query in indexes:
        try:
            updater._execute_query(index_query)
            # logger.info(f"Created index: {index_query.split('FOR')[1].split('ON')[0].strip()}")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")
    
    updater.close()
    # logger.info("Index creation complete")


# def test_single_file_sync(json_file_path: str, neo4j_config: Neo4jConfig):
#     # logger.info(f"Testing sync with file: {json_file_path}")
    
#     try:
#         with open(json_file_path, 'r') as f:
#             aas_data = json.load(f)
        
#         parser = AASParser()
#         kg_updater = KnowledgeGraphUpdater(neo4j_config)
        
#         # Process submodels
#         submodels = aas_data.get('submodels', [])
#         # logger.info(f"Found {len(submodels)} submodels in file")
        
#         for submodel in submodels:
#             parsed_data = parser.parse_submodel(submodel)
#             # logger.info(f"Parsed submodel: {parsed_data['id']}, type: {parsed_data['entity_type']}")
#             # logger.info(f"References found: {list(parsed_data['references'].keys())}")
            
#             if parsed_data['entity_type']:
#                 kg_updater.update_entity(parsed_data)
#             else:
#                 logger.warning(f"Could not determine entity type for {submodel.get('id')}")
        
#         kg_updater.close()
#         # logger.info("Test sync completed successfully")
        
#     except Exception as e:
#         logger.error(f"Error in test sync: {e}")
#         import traceback
#         logger.error(traceback.format_exc())


def main():
    
    # Configuration
    basyx_config = BaSyxConfig()
    neo4j_config = Neo4jConfig()
    
    neo4j_config.URI = "bolt://localhost:7687"
    neo4j_config.USER = "neo4j"
    neo4j_config.PASSWORD = "fVcPu!d3iV#0cT"
    
    
    # Set polling interval (seconds)
    basyx_config.POLL_INTERVAL = 3
    
    try:
        create_indexes(neo4j_config)
    except Exception as e:
        logger.warning(f"Could not create indexes: {e}")
    
    # # Test with individual files:
    # test_single_file_sync('endProduct_test_aas.json', neo4j_config)
    # test_single_file_sync('resource_test_aas.json', neo4j_config)
    # test_single_file_sync('process_test_aas.json', neo4j_config)
    # test_single_file_sync('device_test_aas.json', neo4j_config)
    # return
    
    synchronizer = AAStoKGSynchronizer(basyx_config, neo4j_config)
    
    # # one-time sync:
    # synchronizer.sync_all_aas()
    
    # For continuous sync:
    synchronizer.run_continuous_sync()


if __name__ == "__main__":
    main()

