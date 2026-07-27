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
import argparse
import base64
import glob
import json
import os
import sys
from pathlib import Path

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef, XSD
from rdflib.namespace import OWL

# --------------------------------------------------------------------------
# Pilot-Specific Configuration (Environment-driven)
# --------------------------------------------------------------------------

PILOT = os.getenv("UNIMAAS_PILOT", "adient").lower()

# Pilot namespace mappings
PILOT_NAMESPACES = {
    "adient": {
        "data_ns": "http://unimaas-project.eu/MSCOntology/data/adient/",
        "locations": {
            "Plant Barcelona": "site_37",
            "Plant Valencia": "site_50",
            "Warehouse Madrid": "site_11",
        }
    },
    # "anv": {
    #     "data_ns": "http://unimaas-project.eu/MSCOntology/data/anv/",
    #     "locations": {
    #         "ANV Factory": "site_101",
    #         "Logistics Hub": "site_102",
    #     }
    # },
    # "aegean": {
    #     "data_ns": "http://unimaas-project.eu/MSCOntology/data/aegean/",
    #     "locations": {
    #         "Maintenance Hangar A": "site_201",
    #         "Storage": "site_202",
    #     }
    # },
    # "catone": {
    #     "data_ns": "http://unimaas-project.eu/MSCOntology/data/catone/",
    #     "locations": {
    #         "Catone Warehouse": "site_301",
    #         "Backup Storage": "site_302",
    #     }
    # },
}

# Get pilot config
if PILOT not in PILOT_NAMESPACES:
    print(f"ERROR: Unknown pilot '{PILOT}'. Known pilots: {list(PILOT_NAMESPACES.keys())}", file=sys.stderr)
    sys.exit(1)

PILOT_CONFIG = PILOT_NAMESPACES[PILOT]
SC = Namespace("http://unimaas-project.eu/MSCOntology#")
DATA_NS = Namespace(PILOT_CONFIG["data_ns"])
LOCATION_ALIASES = PILOT_CONFIG["locations"]

PREFIXES = {"sc": SC, f"sc{PILOT}": DATA_NS,
            "owl": OWL, "rdfs": RDFS, "xsd": XSD}

# semanticId prefixes considered "ontology terms"
ONTOLOGY_PREFIXES = (str(SC), str(DATA_NS))

# Export pilot-specific DPP fields (semanticId urn:unimaas:<pilot>:dpp:<name>)
EXPORT_DPP_URN_PROPS = True
DPP_URN_PREFIX = f"urn:unimaas:{PILOT}:dpp:"

# MSC classes (used to recognize rdf:type semanticIds and subclass refinement)
MSC_CLASSES = {
    "ConfigurableEntity", "Product", "CustomerRequirement",
    "ProcessConfiguration", "Process", "Resource", "MaterialResource",
    "HumanResource", "SoftwareResource", "EquipmentResource", "ResourceConf",
    "Supplier", "Site", "Device", "Characteristic", "CharacteristicType",
    "Sensor", "LogisticRoute", "Location",
}

# Classes that can have siteHasResource relationships
RESOURCE_CLASSES = {"Resource", "MaterialResource", "HumanResource",
                    "SoftwareResource", "EquipmentResource", "Product"}

# Object properties of the ontology
OBJECT_PROPERTIES = {
    "satisfiesRequirement", "hasComponent", "requiresProcess", "usesResource",
    "usesResourceConf", "refersToResource", "subClassOf", "superClassOf",
    "hasOutput", "hasInput", "hasPreviousStep", "hasNextStep", "isProvidedBy",
    "provides", "isPerformedAt", "performs", "hasDevice", "hasCharacteristic",
    "hasCharacteristicType", "observes", "isObservedBy", "hasLocation",
    "hasStartingPoint", "hasEndingPoint", "suppliesTo", "siteHasResource",
}

# siteHasResource: Site --siteHasResource--> Resource (from currentLocation)
SITE_HAS_RESOURCE = DATA_NS.siteHasResource

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


# --------------------------------------------------------------------------
# BaSyx access (AAS Part 2 REST API) and offline file loading
# --------------------------------------------------------------------------

def b64url(s: str) -> str:
    """Base64url encode for AAS identifiers."""
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def fetch_paginated(session, url):
    """Iterate a BaSyx paginated collection endpoint."""
    cursor = None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        r = session.get(url, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        result = body.get("result", body if isinstance(body, list) else [])
        yield from result
        cursor = (body.get("paging_metadata") or {}).get("cursor")
        if not cursor:
            break


def load_from_basyx(base_url):
    """Return (shells, submodels_by_id) from a BaSyx AAS Environment."""
    import requests
    s = requests.Session()
    base = base_url.rstrip("/")
    print(f"[INFO] Loading from BaSyx: {base}", file=sys.stderr)
    shells = list(fetch_paginated(s, f"{base}/shells"))
    submodels = {sm["id"]: sm for sm in fetch_paginated(s, f"{base}/submodels")}
    # Fetch any referenced submodel not returned by the collection endpoint
    for shell in shells:
        for ref in shell.get("submodels", []):
            for key in ref.get("keys", []):
                if key.get("type") == "Submodel" and key["value"] not in submodels:
                    r = s.get(f"{base}/submodels/{b64url(key['value'])}", timeout=30)
                    if r.ok:
                        submodels[key["value"]] = r.json()
    return shells, submodels


def load_from_files(paths):
    """Load AAS shells and submodels from JSON files."""
    shells, submodels = [], {}
    for pattern in paths:
        for p in sorted(glob.glob(pattern)):
            print(f"[INFO] Loading from file: {p}", file=sys.stderr)
            env = json.loads(Path(p).read_text())
            shells.extend(env.get("assetAdministrationShells", []))
            for sm in env.get("submodels", []):
                submodels[sm["id"]] = sm
    return shells, submodels


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def semantic_iri(element):
    """First GlobalReference key value of the element's semanticId, or None."""
    sid = element.get("semanticId") or {}
    for key in sid.get("keys", []):
        return key.get("value")
    return None


def is_ontology_iri(iri):
    """Check if IRI is from MSC Ontology or pilot namespace."""
    return bool(iri) and (iri.startswith(str(SC)) or iri.startswith(str(DATA_NS)))


def local_name(iri):
    """Extract local name from IRI."""
    if not iri:
        return None
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def subject_iri_for_shell(shell):
    """scadient:<lowercased AAS idShort>."""
    return DATA_NS[shell["idShort"].strip().lower()]


def to_literal(value, value_type):
    """Convert AAS value to RDF Literal with correct datatype."""
    if value_type not in XSD_MAP:
        value_type = "xs:string"
    return Literal(value, datatype=XSD_MAP[value_type])


# --------------------------------------------------------------------------
# Main Exporter Class
# --------------------------------------------------------------------------

class Exporter:
    """Export AAS shells and submodels to MSC RDF graph."""

    def __init__(self, shells, submodels):
        self.shells = shells
        self.submodels = submodels

        self.g = Graph()
        for pfx, ns in PREFIXES.items():
            self.g.bind(pfx, ns)

        # id → subject IRI maps for resolving ModelReferences
        self.subject_by_aas_id = {}
        self.subject_by_submodel_id = {}
        self.types_by_subject = {}

        # Matching keys for currentLocation → Site
        self.site_by_name = {}
        # (resource subject, location string) pairs to resolve at the end
        self.pending_locations = []

    # -- pass 1: register every shell ------------------------------------

    def register_shells(self):
        """Register all shells and build id→IRI mappings."""
        for shell in self.shells:
            subj = subject_iri_for_shell(shell)
            self.subject_by_aas_id[shell["id"]] = subj
            types = set()
            iri = semantic_iri(shell)
            if is_ontology_iri(iri) and local_name(iri) in MSC_CLASSES:
                types.add(local_name(iri))
            self.types_by_subject[subj] = types
            # Register submodels attached to this shell
            for ref in shell.get("submodels", []):
                for key in ref.get("keys", []):
                    if key.get("type") == "Submodel":
                        self.subject_by_submodel_id[key["value"]] = subj

    def resolve_reference(self, ref):
        """ReferenceElement value → object IRI (or None)."""
        if not ref:
            return None
        keys = ref.get("keys", [])
        if not keys:
            return None
        if ref.get("type") == "ExternalReference":
            v = keys[0].get("value", "")
            return URIRef(v) if v.startswith(("http://", "https://")) else None
        # ModelReference: walk keys, resolve AAS or Submodel ids
        for key in keys:
            v = key.get("value")
            if key.get("type") == "AssetAdministrationShell" and v in self.subject_by_aas_id:
                return self.subject_by_aas_id[v]
            if key.get("type") == "Submodel" and v in self.subject_by_submodel_id:
                return self.subject_by_submodel_id[v]
        return None

    # -- pass 2: walk every shell's submodels -----------------------------

    def export(self):
        """Export all shells and submodels to RDF graph."""
        self.register_shells()
        for shell in self.shells:
            subj = self.subject_by_aas_id[shell["id"]]
            for ref in shell.get("submodels", []):
                for key in ref.get("keys", []):
                    sm = self.submodels.get(key.get("value"))
                    if sm:
                        self.walk_submodel(subj, sm)
        self.emit_types()
        self.emit_site_has_resource()
        self.emit_property_declarations()
        return self.g

    def walk_submodel(self, subj, sm):
        """Walk a submodel's elements."""
        iri = semantic_iri(sm)
        if is_ontology_iri(iri) and local_name(iri) in MSC_CLASSES:
            self.types_by_subject[subj].add(local_name(iri))
        for el in sm.get("submodelElements", []):
            self.walk_element(subj, el)

    def walk_element(self, subj, el):
        """Walk a single element (Property, Reference, Collection, etc.)."""
        mtype = el.get("modelType")
        iri = semantic_iri(el)

        if mtype == "Property":
            self.handle_property(subj, el, iri)

        elif mtype == "MultiLanguageProperty" and is_ontology_iri(iri):
            for entry in el.get("value", []):
                self.g.add((subj, URIRef(iri),
                            Literal(entry["text"], lang=entry.get("language"))))

        elif mtype == "ReferenceElement" and is_ontology_iri(iri):
            if local_name(iri) in OBJECT_PROPERTIES:
                obj = self.resolve_reference(el.get("value"))
                if obj is not None:
                    self.g.add((subj, URIRef(iri), obj))

        elif mtype in ("SubmodelElementCollection", "SubmodelElementList"):
            # Recurse; children carry their own semanticIds
            for child in el.get("value", []):
                self.walk_element(subj, child)

    def handle_property(self, subj, el, iri):
        """Handle a Property element."""
        value = el.get("value")
        if value in (None, ""):
            return
        name = local_name(iri) if iri else None

        # SiteName: special key for location matching (not exported)
        if iri == "urn:unimaas:adient:aas:siteName" or el.get("idShort") == "siteName":
            self.site_by_name[value.strip().lower()] = subj
            return
        if iri == f"urn:unimaas:{PILOT}:aas:siteName" or el.get("idShort") == "siteName":
            self.site_by_name[value.strip().lower()] = subj
            return

        if is_ontology_iri(iri):
            # Class refinement: e.g., ResourceType = "EquipmentResource"
            if name in MSC_CLASSES:
                if value in MSC_CLASSES:
                    self.types_by_subject[subj].add(value)
                return

            # Object property carried as plain string (currentLocation)
            if name in OBJECT_PROPERTIES:
                if name == "hasLocation":
                    self.pending_locations.append((subj, value.strip()))
                return

            # Plain datatype property
            self.g.add((subj, URIRef(iri),
                        to_literal(value, el.get("valueType", "xs:string"))))

        elif EXPORT_DPP_URN_PROPS and iri and iri.startswith(DPP_URN_PREFIX):
            # Pilot-specific URN properties → scadient:fieldname
            pred = DATA_NS[iri[len(DPP_URN_PREFIX):]]
            self.g.add((subj, pred,
                        to_literal(value, el.get("valueType", "xs:string"))))

    # -- pass 3: finishing touches ----------------------------------------

    def emit_types(self):
        """Emit all rdf:type triples."""
        for subj, types in self.types_by_subject.items():
            for t in types:
                self.g.add((subj, RDF.type, SC[t]))

    def emit_site_has_resource(self):
        """Emit siteHasResource relationships for containers/resources at locations."""
        for resource, loc in self.pending_locations:
            # Only emit siteHasResource for Resource-typed entities
            if not self.types_by_subject.get(resource, set()) & RESOURCE_CLASSES:
                continue

            # Resolve location name to site via aliases
            key = LOCATION_ALIASES.get(loc, loc).strip().lower()
            site = self.site_by_name.get(key)

            if site is None:
                print(f"[WARN] currentLocation '{loc}' of {resource} "
                      f"matches no Site. Add to LOCATION_ALIASES in config.",
                      file=sys.stderr)
                continue

            # Emit: site --siteHasResource--> resource
            self.g.add((site, SITE_HAS_RESOURCE, resource))

    def emit_property_declarations(self):
        """Declare dataset-level property definitions."""
        used = {p for _, p, _ in self.g}

        if DATA_NS.suppliesTo in used:
            self.g.add((DATA_NS.suppliesTo, RDF.type, OWL.ObjectProperty))
            self.g.add((DATA_NS.suppliesTo, RDFS.label, Literal("supplies to")))
            self.g.add((DATA_NS.suppliesTo, RDFS.comment,
                        Literal("Direct supply/delivery flow from one site to another")))
            self.g.add((DATA_NS.suppliesTo, RDFS.domain, SC.Site))
            self.g.add((DATA_NS.suppliesTo, RDFS.range, SC.Site))

        if SITE_HAS_RESOURCE in used:
            self.g.add((SITE_HAS_RESOURCE, RDF.type, OWL.ObjectProperty))
            self.g.add((SITE_HAS_RESOURCE, RDFS.label, Literal("site has resource")))
            self.g.add((SITE_HAS_RESOURCE, RDFS.comment,
                        Literal("Links a Site to the Resources (e.g., containers) "
                                "currently located at it, derived from DPP currentLocation")))
            self.g.add((SITE_HAS_RESOURCE, RDFS.domain, SC.Site))
            self.g.add((SITE_HAS_RESOURCE, RDFS.range, SC.Resource))


# --------------------------------------------------------------------------
# CLI & Main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    global PILOT, PILOT_CONFIG, DATA_NS, LOCATION_ALIASES, DPP_URN_PREFIX
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--basyx", metavar="URL",
                     help="BaSyx AAS Environment base URL (e.g., http://localhost:8081)")
    src.add_argument("--files", nargs="+", metavar="GLOB",
                     help="AAS environment JSON files (offline mode, e.g., aas_*.json)")

    ap.add_argument("--pilot", default=PILOT,
                    help=f"Pilot name ({', '.join(PILOT_NAMESPACES.keys())}). "
                         f"Default: $UNIMAAS_PILOT or 'adient'")
    ap.add_argument("-o", "--output", default="msc_dataset.ttl",
                    help="Output RDF Turtle file (default: msc_dataset.ttl)")

    args = ap.parse_args()

    # Override pilot if specified
    
    PILOT = args.pilot.lower()
    if PILOT not in PILOT_NAMESPACES:
        print(f"ERROR: Unknown pilot '{PILOT}'. Known: {list(PILOT_NAMESPACES.keys())}", 
              file=sys.stderr)
        sys.exit(1)
    PILOT_CONFIG = PILOT_NAMESPACES[PILOT]
    DATA_NS = Namespace(PILOT_CONFIG["data_ns"])
    LOCATION_ALIASES.update(PILOT_CONFIG["locations"])
    DPP_URN_PREFIX = f"urn:unimaas:{PILOT}:dpp:"

    print(f"[INFO] Pilot: {PILOT}", file=sys.stderr)
    print(f"[INFO] Data namespace: {DATA_NS}", file=sys.stderr)

    if args.basyx:
        shells, submodels = load_from_basyx(args.basyx)
    else:
        shells, submodels = load_from_files(args.files)

    print(f"[INFO] Loaded {len(shells)} shells, {len(submodels)} submodels", file=sys.stderr)

    g = Exporter(shells, submodels).export()

    g.serialize(destination=args.output, format="turtle")
    print(f"[INFO] Wrote {len(g)} triples to {args.output}", file=sys.stderr)
    print(f"Generated RDF dataset: {args.output}", file=sys.stdout)


if __name__ == "__main__":
    main()
