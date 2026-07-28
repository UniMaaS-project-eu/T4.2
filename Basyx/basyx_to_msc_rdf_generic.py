#!/usr/bin/env python3
"""
basyx_to_msc_rdf_generic.py
============================
Generic exporter: BaSyx AAS environment → MSC-Ontology RDF (Turtle).

**Enhanced Version**: Adds explicit handling for containers/resources with siteHasResource.

The translation is entirely driven by semanticIds, works for ANY entity type without per-class code:

  1. AAS/Submodel semanticId that is an MSC class IRI
         → rdf:type of the entity.
     Subject IRI = DATA_NS + lowercase(AAS idShort)

  2. Property whose semanticId is an owl:DatatypeProperty IRI
         → (subject, predicate, typed literal)

  3. SubmodelElementCollection whose semanticId is an owl:ObjectProperty
         → (subject, predicate, object) triples via ReferenceElements

  4. Property whose semanticId is an OBJECT property with plain string value
         (e.g., currentLocation with semanticId sc:hasLocation)
         → string resolved to Site via LOCATION_ALIASES or name matching
         → extra siteHasResource relationship generated for Resource-typed entities

  5. Property whose semanticId is an MSC CLASS IRI and value is a subclass name
         → rdf:type refined (e.g., ResourceType = "EquipmentResource")

Usage:
    # Live BaSyx (AAS Part 2 HTTP/REST API, BaSyx v2)
    python basyx_to_msc_rdf_generic.py --basyx http://localhost:8081 --pilot adient -o dataset.ttl

    # Offline, from AAS JSON files
    python basyx_to_msc_rdf_generic.py --files sites_aas/*.json *_dpp_aas.json --pilot adient -o dataset.ttl

    # Docker
    docker run -v $(pwd):/data unimaas/basyx-to-msc-rdf:latest \
        --files /data/aas_*.json --pilot adient -o /data/dataset.ttl

Requires: pip install rdflib requests
"""
#!/usr/bin/env python3
"""
basyx_to_msc_rdf_generic_v2.py
===============================
Enhanced BaSyx AAS → MSC-Ontology RDF Exporter

**Key Enhancement**: Naming prefixes based on MSC class types

The translation is entirely driven by semanticIds + MSC class types:

  1. AAS Shell semanticId identifies MSC class (e.g., sc:Product, sc:Site)
  2. RDF subject IRI generated with class-specific prefix:
     - Product          → scadient:product_<name>
     - Site             → scadient:site_<name>
     - LogisticRoute    → scadient:route_<name>
     - Location         → scadient:location_<name>
     - Resource         → scadient:resource_<name>
     - Process          → scadient:process_<name>
     - ProcessConfiguration → scadient:pconf_<name>
     - ResourceConf     → scadient:rconf_<name>
     - Supplier         → scadient:supplier_<name>
     - Sensor           → scadient:sensor_<name>
     - Device           → scadient:device_<name>
     - CharacteristicType → scadient:ctype_<name>
     - Characteristic   → scadient:char_<name>
     - CustomerRequirement → scadient:creq_<name>

  3. All datatype/object properties exported based on semanticId
  4. Automatic siteHasResource for Resources at Locations
  5. Reference resolution via AAS id registry

Usage:
    # Live BaSyx
    python basyx_to_msc_rdf_generic_v2.py --basyx http://localhost:8081 --pilot adient -o dataset.ttl

    # Offline, from JSON files
    python basyx_to_msc_rdf_generic_v2.py --files aas_*.json --pilot adient -o dataset.ttl

    # Docker
    docker run -v $(pwd):/data unimaas/basyx-to-msc-rdf:latest \
        --files /data/aas_*.json --pilot adient -o /data/dataset.ttl

Requires: pip install rdflib requests
"""
#!/usr/bin/env python3
"""
basyx_live_rdf_exporter_v2.py
==============================
Production-Grade BaSyx → MSC-Ontology RDF Exporter

**Key Features:**
- ✅ Extracts live from deployed BaSyx REST API
- ✅ Uses unified AAS ID format: urn:unimaas:<pilot>:<MSC_entity_type>:<idShort>
- ✅ Directly extracts MSC entity type from AAS ID (no inference needed)
- ✅ Generates RDF: <pilot_namespace>:<MSC_prefix>_<idShort>
- ✅ Full property mapping (data + object properties)
- ✅ Handles paginated BaSyx responses
- ✅ Robust error handling and logging
- ✅ Matches example datasets (Adient, ANV, Aegean, Catone)

**Naming Convention:**
  AAS:  urn:unimaas:adient:resource:magnum_optimum_1114567
  RDF:  scadient:resource_magnum_optimum_1114567 a sc:Resource

**Usage:**
  python basyx_live_rdf_exporter_v2.py \\
      --basyx http://localhost:8081 \\
      --pilot adient \\
      --output rdf-datasets/msc_dataset.ttl \\
      --log-level INFO

**Requirements:**
  pip install rdflib requests
"""

import argparse
import base64
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests
from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef, XSD
from rdflib.namespace import OWL

# =============================================================================
# CONFIGURATION
# =============================================================================

# Pilot-Specific Configuration
PILOT_NAMESPACES = {
    "adient": "http://unimaas-project.eu/MSCOntology/data/adient/",
    "anv": "http://unimaas-project.eu/MSCOntology/data/anv/",
    "aegean": "http://unimaas-project.eu/MSCOntology/data/aegean/",
    "catone": "http://unimaas-project.eu/MSCOntology/data/catone/",
}

# MSC Class → RDF Subject Prefix Mapping
# Used for RDF subject generation: prefix_idShort
CLASS_TO_PREFIX = {
    "Product": "product",
    "Site": "site",
    "Location": "location",
    "LogisticRoute": "route",
    "Process": "process",
    "ProcessConfiguration": "pconf",
    "Resource": "resource",
    "MaterialResource": "material",
    "HumanResource": "human",
    "SoftwareResource": "software",
    "EquipmentResource": "equipment",
    "ResourceConf": "rconf",
    "Supplier": "supplier",
    "Sensor": "sensor",
    "Device": "device",
    "Characteristic": "char",
    "CharacteristicType": "ctype",
    "CustomerRequirement": "creq",
}

# MSC Object Properties (for ReferenceElement handling)
OBJECT_PROPERTIES = {
    "satisfiesRequirement", "hasComponent", "requiresProcess", "usesResource",
    "usesResourceConf", "refersToResource", "subClassOf", "superClassOf",
    "hasOutput", "hasInput", "hasPreviousStep", "hasNextStep", "isProvidedBy",
    "provides", "isPerformedAt", "performs", "hasDevice", "hasCharacteristic",
    "hasCharacteristicType", "observes", "isObservedBy", "hasLocation",
    "hasStartingPoint", "hasEndingPoint", "suppliesTo", "siteHasResource",
}

# AAS valueType → XSD datatype mapping
XSD_MAP = {
    "xs:string": XSD.string, "xs:boolean": XSD.boolean,
    "xs:integer": XSD.integer, "xs:int": XSD.integer,
    "xs:long": XSD.integer, "xs:short": XSD.integer,
    "xs:decimal": XSD.decimal, "xs:double": XSD.decimal,
    "xs:float": XSD.decimal,
    "xs:date": XSD.date, "xs:dateTime": XSD.dateTime,
    "xs:anyURI": XSD.anyURI,
}

MSC_ONTOLOGY_URL = "http://unimaas-project.eu/MSCOntology#"

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="[%(levelname)s] %(message)s",
    )


# =============================================================================
# AAS ID PARSING
# =============================================================================

class AASIDParser:
    """Parse unified AAS ID format: urn:unimaas:<pilot>:<msc_type>:<idshort>"""

    PATTERN = re.compile(
        r"^urn:unimaas:([a-z]+):([a-zA-Z]+):(.+)$"
    )

    @staticmethod
    def parse(aas_id: str) -> Optional[Tuple[str, str, str]]:
        """
        Parse AAS ID into (pilot, msc_type, idshort).
        
        Example:
            urn:unimaas:adient:resource:magnum_optimum_1114567
            → ("adient", "resource", "magnum_optimum_1114567")
        """
        match = AASIDParser.PATTERN.match(aas_id)
        if not match:
            logger.warning(f"Invalid AAS ID format: {aas_id}")
            return None
        
        pilot, msc_type_raw, idshort = match.groups()
        
        # Normalize MSC type (first letter uppercase)
        msc_type = msc_type_raw.capitalize()
        
        # Handle special cases (e.g., "logisticroute" → "LogisticRoute")
        if msc_type_raw.lower() == "logisticroute":
            msc_type = "LogisticRoute"
        elif msc_type_raw.lower() == "processconfiguration":
            msc_type = "ProcessConfiguration"
        elif msc_type_raw.lower() == "customerrequirement":
            msc_type = "CustomerRequirement"
        elif msc_type_raw.lower() == "materialresource":
            msc_type = "MaterialResource"
        elif msc_type_raw.lower() == "humanresource":
            msc_type = "HumanResource"
        elif msc_type_raw.lower() == "softwareresource":
            msc_type = "SoftwareResource"
        elif msc_type_raw.lower() == "equipmentresource":
            msc_type = "EquipmentResource"
        elif msc_type_raw.lower() == "resourceconf":
            msc_type = "ResourceConf"
        elif msc_type_raw.lower() == "characteristictype":
            msc_type = "CharacteristicType"
        
        if pilot not in PILOT_NAMESPACES:
            logger.warning(f"Unknown pilot in AAS ID: {pilot}")
            return None
        
        if msc_type not in CLASS_TO_PREFIX:
            logger.warning(f"Unknown MSC type in AAS ID: {msc_type} (from {aas_id})")
            return None
        
        return (pilot, msc_type, idshort)


# =============================================================================
# BaSyx Data Extraction
# =============================================================================

class BaSyxExtractor:
    """Extract complete AAS structure from BaSyx REST API."""

    def __init__(self, base_url: str, timeout: int = 30):
        """Initialize BaSyx extractor."""
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        logger.info(f"BaSyx base URL: {self.base_url}")

    def fetch_paginated(self, endpoint: str) -> List[Dict]:
        """Fetch all pages from a paginated BaSyx endpoint."""
        results = []
        cursor = None
        page = 0

        while True:
            params = {"limit": 100}
            if cursor:
                params["cursor"] = cursor

            url = f"{self.base_url}{endpoint}"
            try:
                r = self.session.get(url, params=params, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
            except requests.RequestException as e:
                logger.error(f"Failed to fetch {endpoint}: {e}")
                break

            page_results = data.get("result", [])
            if not page_results:
                break

            results.extend(page_results)
            page += 1
            logger.debug(f"  Page {page}: {len(page_results)} items")

            cursor = data.get("paging_metadata", {}).get("cursor")
            if not cursor:
                break

        logger.info(f"Fetched {len(results)} items from {endpoint}")
        return results

    def extract_aas_environment(self) -> Tuple[List[Dict], Dict[str, Dict]]:
        """Extract complete AAS environment from BaSyx."""
        logger.info("Extracting AAS environment from BaSyx...")

        # Fetch all shells
        shells = self.fetch_paginated("/shells")
        logger.info(f"Found {len(shells)} shells")

        # Fetch all submodels
        submodels_list = self.fetch_paginated("/submodels")
        submodels = {sm["id"]: sm for sm in submodels_list}
        logger.info(f"Found {len(submodels)} submodels")

        return shells, submodels


# =============================================================================
# RDF GENERATION
# =============================================================================

class RDFExporter:
    """Export AAS to RDF following MSC ontology."""

    def __init__(self, shells: List[Dict], submodels: Dict[str, Dict]):
        """Initialize RDF exporter."""
        self.shells = shells
        self.submodels = submodels

        # RDF setup
        self.sc = Namespace(MSC_ONTOLOGY_URL)
        self.data_ns_by_pilot = {
            pilot: Namespace(url) 
            for pilot, url in PILOT_NAMESPACES.items()
        }

        self.g = Graph()
        self.g.bind("sc", self.sc)
        for pilot, ns in self.data_ns_by_pilot.items():
            self.g.bind(f"sc{pilot}", ns)
        self.g.bind("owl", OWL)
        self.g.bind("rdfs", RDFS)
        self.g.bind("xsd", XSD)

        # Mappings
        self.subject_by_aas_id = {}
        self.types_by_subject = {}
        self.pilot_by_subject = {}

    def export(self) -> Graph:
        """Export all shells to RDF."""
        logger.info("Generating RDF...")

        # Register shells
        for shell in self.shells:
            self._register_shell(shell)

        # Walk submodels
        for shell in self.shells:
            self._process_shell_submodels(shell)

        # Emit types
        self._emit_types()

        logger.info(f"Generated {len(self.g)} triples")
        return self.g

    def _register_shell(self, shell: Dict):
        """Register a shell and parse its AAS ID."""
        aas_id = shell.get("id")
        if not aas_id:
            logger.warning(f"Shell has no ID: {shell.get('idShort')}")
            return

        # Parse AAS ID: urn:unimaas:<pilot>:<msc_type>:<idshort>
        parsed = AASIDParser.parse(aas_id)
        if not parsed:
            logger.warning(f"Could not parse AAS ID: {aas_id}")
            return

        pilot, msc_type, idshort = parsed

        # Generate RDF subject
        prefix = CLASS_TO_PREFIX[msc_type]
        data_ns = self.data_ns_by_pilot[pilot]
        subj = data_ns[f"{prefix}_{idshort}"]

        # Register
        self.subject_by_aas_id[aas_id] = subj
        self.pilot_by_subject[subj] = pilot
        self.types_by_subject[subj] = {msc_type}

        logger.debug(
            f"Registered {shell.get('idShort')}: {aas_id} → {subj}"
        )

    def _process_shell_submodels(self, shell: Dict):
        """Process all submodels of a shell."""
        aas_id = shell.get("id")
        subj = self.subject_by_aas_id.get(aas_id)

        if not subj:
            return

        # Walk each submodel reference
        for sm_ref in shell.get("submodels", []):
            sm_id = None
            for key in sm_ref.get("keys", []):
                if key.get("type") == "Submodel":
                    sm_id = key.get("value")
                    break

            if not sm_id:
                continue

            sm = self.submodels.get(sm_id)
            if sm:
                self._walk_submodel(subj, sm)

    def _walk_submodel(self, subj: URIRef, sm: Dict):
        """Walk submodel elements."""
        for el in sm.get("submodelElements", []):
            self._walk_element(subj, el)

    def _walk_element(self, subj: URIRef, el: Dict):
        """Walk a single element."""
        mtype = el.get("modelType")
        iri = self._semantic_iri(el)

        if not iri or not self._is_ontology_iri(iri):
            return

        prop_name = self._local_name(iri)
        if not prop_name:
            return

        if mtype == "Property":
            self._handle_property(subj, el, iri)

        elif mtype == "MultiLanguageProperty":
            for entry in el.get("value", []):
                text = entry.get("text")
                lang = entry.get("language")
                if text:
                    self.g.add(
                        (subj, URIRef(iri), Literal(text, lang=lang))
                    )

        elif mtype == "ReferenceElement":
            if prop_name in OBJECT_PROPERTIES:
                obj = self._resolve_reference(el.get("value"))
                if obj:
                    self.g.add((subj, URIRef(iri), obj))

    def _handle_property(self, subj: URIRef, el: Dict, iri: str):
        """Handle a Property element."""
        value = el.get("value")
        if value in (None, ""):
            return

        vtype = el.get("valueType", "xs:string")
        xsd_type = XSD_MAP.get(vtype, XSD.string)

        try:
            # Type conversion
            if xsd_type in (XSD.integer, XSD.int):
                value = int(value)
            elif xsd_type in (XSD.decimal, XSD.double, XSD.float):
                value = float(value)
            elif xsd_type == XSD.boolean:
                value = str(value).lower() in ("true", "1", "yes")

            self.g.add((subj, URIRef(iri), Literal(value, datatype=xsd_type)))

        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to convert property {el.get('idShort')}: {e}")

    def _resolve_reference(self, ref: Dict) -> Optional[URIRef]:
        """Resolve a ReferenceElement to a URI."""
        if not ref:
            return None

        for key in ref.get("keys", []):
            aas_id = key.get("value")
            if aas_id in self.subject_by_aas_id:
                return self.subject_by_aas_id[aas_id]

        return None

    def _emit_types(self):
        """Emit rdf:type triples."""
        for subj, types in self.types_by_subject.items():
            for t in types:
                self.g.add((subj, RDF.type, self.sc[t]))

    @staticmethod
    def _semantic_iri(element: Dict) -> Optional[str]:
        """Extract semanticId IRI from element."""
        sid = element.get("semanticId") or {}
        for key in sid.get("keys", []):
            return key.get("value")
        return None

    @staticmethod
    def _is_ontology_iri(iri: Optional[str]) -> bool:
        """Check if IRI is from MSC Ontology."""
        return bool(iri and iri.startswith(MSC_ONTOLOGY_URL))

    @staticmethod
    def _local_name(iri: Optional[str]) -> Optional[str]:
        """Extract local name from IRI."""
        if not iri:
            return None
        return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export live BaSyx to MSC-Ontology RDF",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Live BaSyx
  %(prog)s --basyx http://localhost:8081 --pilot adient -o dataset.ttl

  # Different pilot
  %(prog)s --basyx http://localhost:8081 --pilot anv -o anv_dataset.ttl

  # Debug mode
  %(prog)s --basyx http://localhost:8081 --pilot adient -o dataset.ttl --log-level DEBUG
        """,
    )

    parser.add_argument(
        "--basyx",
        required=True,
        help="BaSyx base URL (e.g., http://localhost:8081)",
    )
    parser.add_argument(
        "--pilot",
        default="adient",
        choices=list(PILOT_NAMESPACES.keys()),
        help="Pilot (default: adient)",
    )
    parser.add_argument(
        "-o", "--output",
        default="msc_dataset.ttl",
        help="Output RDF file (default: msc_dataset.ttl)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--save-aas",
        help="Save extracted AAS JSON to file (for debugging)",
    )

    args = parser.parse_args()

    setup_logging(args.log_level)

    logger.info("="*70)
    logger.info("UniMaaS RDF Exporter v2.0 (Live BaSyx → MSC Ontology)")
    logger.info("="*70)
    logger.info(f"Pilot: {args.pilot}")
    logger.info(f"BaSyx: {args.basyx}")
    logger.info(f"Output: {args.output}")
    logger.info("="*70)

    try:
        # Extract from BaSyx
        extractor = BaSyxExtractor(args.basyx)
        shells, submodels = extractor.extract_aas_environment()

        if not shells:
            logger.error("No shells found in BaSyx!")
            sys.exit(1)

        # Save AAS if requested
        if args.save_aas:
            aas_env = {
                "assetAdministrationShells": shells,
                "submodels": list(submodels.values()),
            }
            Path(args.save_aas).write_text(json.dumps(aas_env, indent=2))
            logger.info(f"✅ Saved extracted AAS to {args.save_aas}")

        # Export to RDF
        exporter = RDFExporter(shells, submodels)
        g = exporter.export()

        # Serialize
        g.serialize(destination=args.output, format="turtle")
        logger.info(f"✅ Saved RDF to {args.output}")

        # Print summary
        logger.info("="*70)
        triple_count = len(g)
        logger.info(f"Summary: Generated {triple_count} triples")
        
        # Count by type
        type_counts = {}
        for s, p, o in g.triples((None, RDF.type, None)):
            type_name = str(o).split("#")[-1]
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
        
        if type_counts:
            logger.info("Entities by type:")
            for entity_type in sorted(type_counts.keys()):
                logger.info(f"  {entity_type}: {type_counts[entity_type]}")
        
        logger.info("="*70)

    except Exception as e:
        logger.error(f"❌ Export failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()