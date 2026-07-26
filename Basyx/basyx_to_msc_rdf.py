#!/usr/bin/env python3
"""
basyx_to_msc_rdf.py
===================
Generic exporter: BaSyx AAS environment  ->  MSC-Ontology RDF (Turtle).

The translation is entirely driven by semanticIds, so it works for ANY
entity type (Site, Resource, Process, Supplier, ...) without per-class code:

  1. AAS/Submodel semanticId that is an MSC class IRI
         -> rdf:type of the entity.
     Subject IRI = DATA_NS + lowercase(AAS idShort)   (Site_11 -> sca:site_11)

  2. Property whose semanticId is an owl:DatatypeProperty IRI (http/https)
         -> (subject, predicate, typed literal); AAS valueType -> XSD datatype.

  3. SubmodelElementCollection whose semanticId is an owl:ObjectProperty IRI,
     containing ReferenceElements
         -> (subject, predicate, object) triples.
       * ExternalReference/GlobalReference -> the IRI is used directly.
       * ModelReference (AAS or Submodel key) -> resolved to the subject IRI
         of that shell inside BaSyx.

  4. Property whose semanticId is an OBJECT property but whose value is a
     plain string (e.g. DPP "currentLocation" with semanticId sc:hasLocation)
         -> the string is resolved to a Site via the SiteName matching key /
            LOCATION_ALIASES, and the extra relationship
                (site, sca:siteHasResource, resource)
            is emitted for every Resource-typed entity.

  5. Property whose semanticId is an MSC CLASS IRI and whose value names a
     subclass (e.g. ResourceType = "EquipmentResource", semanticId sc:Resource)
         -> refines rdf:type to sc:EquipmentResource.

Usage:
    # live BaSyx (AAS Part 2 HTTP/REST API, BaSyx v2)
    python basyx_to_msc_rdf.py --basyx http://localhost:8081 -o dataset.ttl

    # offline, from AAS environment JSON files
    python basyx_to_msc_rdf.py --files sites_aas/*.json *_dpp_aas.json -o dataset.ttl

Requires:  pip install rdflib requests
"""
import argparse
import base64
import glob
import json
import sys
from pathlib import Path

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef, XSD
from rdflib.namespace import OWL

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
SC = Namespace("http://unimaas-project.eu/MSCOntology#")
DATA_NS = Namespace("http://unimaas-project.eu/MSCOntology/data/adient/")
PREFIXES = {"sc": SC, "scadient": DATA_NS,
            "owl": OWL, "rdfs": RDFS, "xsd": XSD}

# semanticId prefixes considered "ontology terms" (exported as predicates/types)
ONTOLOGY_PREFIXES = (str(SC), str(DATA_NS))

# Optionally also export the pilot-specific DPP fields
# (semanticId "urn:unimaas:adient:dpp:<name>") as scadient:<name> literals.
EXPORT_DPP_URN_PROPS = True
DPP_URN_PREFIX = "urn:unimaas:adient:dpp:"

# MSC classes (used to recognise rdf:type semanticIds and subclass refinement)
MSC_CLASSES = {
    "ConfigurableEntity", "Product", "CustomerRequirement",
    "ProcessConfiguration", "Process", "Resource", "MaterialResource",
    "HumanResource", "SoftwareResource", "EquipmentResource", "ResourceConf",
    "Supplier", "Site", "Device", "Characteristic", "CharacteristicType",
    "Sensor", "LogisticRoute", "Location",
}
RESOURCE_CLASSES = {"Resource", "MaterialResource", "HumanResource",
                    "SoftwareResource", "EquipmentResource"}

# Object properties of the ontology whose targets are entities, not literals.
# (Everything else with an sc:/scadient: semanticId is treated as datatype.)
OBJECT_PROPERTIES = {
    "satisfiesRequirement", "hasComponent", "requiresProcess", "usesResource",
    "usesResourceConf", "refersToResource", "subClassOf", "superClassOf",
    "hasOutput", "hasInput", "hasPreviousStep", "hasNextStep", "isProvidedBy",
    "provides", "isPerformedAt", "performs", "hasDevice", "hasCharacteristic",
    "hasCharacteristicType", "observes", "isObservedBy", "hasLocation",
    "hasStartingPoint", "hasEndingPoint", "suppliesTo", "siteHasResource",
}

# The extra relationship requested: Site --siteHasResource--> Resource,
# derived from the resource's currentLocation (semanticId sc:hasLocation).
SITE_HAS_RESOURCE = DATA_NS.siteHasResource

# Free-text currentLocation values -> Site local name (adjust to your plants!)
LOCATION_ALIASES = {
    "Plant Barcelona": "site_37",   # ES plant
    "Plant Valencia": "site_50",
    "Warehouse Madrid": "site_11",
}

# AAS valueType -> XSD datatype (decimals kept as xsd:decimal to match the
# ontology ranges and the plain-number Turtle style of the target dataset)
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
    shells, submodels = [], {}
    for pattern in paths:
        for p in sorted(glob.glob(pattern)):
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
    return bool(iri) and iri.startswith(ONTOLOGY_PREFIXES)


def local_name(iri):
    return iri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def subject_iri_for_shell(shell):
    """scadient:<lowercased AAS idShort>  (Site_11 -> scadient:site_11)."""
    return DATA_NS[shell["idShort"].strip().lower()]


def to_literal(value, value_type):
    dt = XSD_MAP.get(value_type, XSD.string)
    if dt == XSD.string:
        return Literal(value)
    return Literal(value, datatype=dt)


# --------------------------------------------------------------------------
# Core translation
# --------------------------------------------------------------------------
class Exporter:
    def __init__(self, shells, submodels):
        self.shells = shells
        self.submodels = submodels
        self.g = Graph()
        for pfx, ns in PREFIXES.items():
            self.g.bind(pfx, ns)

        # id -> subject IRI maps, for resolving ModelReferences
        self.subject_by_aas_id = {}
        self.subject_by_submodel_id = {}
        self.types_by_subject = {}

        # matching keys for currentLocation -> Site
        self.site_by_name = {}
        # (resource subject, location string) pairs to resolve at the end
        self.pending_locations = []

    # -- pass 1: register every shell ------------------------------------
    def register_shells(self):
        for shell in self.shells:
            subj = subject_iri_for_shell(shell)
            self.subject_by_aas_id[shell["id"]] = subj
            types = set()
            iri = semantic_iri(shell)
            if is_ontology_iri(iri) and local_name(iri) in MSC_CLASSES:
                types.add(local_name(iri))
            self.types_by_subject[subj] = types
            for ref in shell.get("submodels", []):
                for key in ref.get("keys", []):
                    if key.get("type") == "Submodel":
                        self.subject_by_submodel_id[key["value"]] = subj

    def resolve_reference(self, ref):
        """ReferenceElement value -> object IRI (or None)."""
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
        iri = semantic_iri(sm)
        if is_ontology_iri(iri) and local_name(iri) in MSC_CLASSES:
            self.types_by_subject[subj].add(local_name(iri))
        for el in sm.get("submodelElements", []):
            self.walk_element(subj, el)

    def walk_element(self, subj, el):
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
            # Recurse; children carry their own semanticIds.
            for child in el.get("value", []):
                self.walk_element(subj, child)

    def handle_property(self, subj, el, iri):
        value = el.get("value")
        if value in (None, ""):
            return
        name = local_name(iri) if iri else None

        # SiteName matching key (not exported): register for location matching
        if iri == "urn:unimaas:adient:aas:siteName" or el.get("idShort") == "SiteName":
            self.site_by_name[value.strip().lower()] = subj
            return

        if is_ontology_iri(iri):
            # class refinement, e.g. ResourceType = "EquipmentResource"
            if name in MSC_CLASSES:
                if value in MSC_CLASSES:
                    self.types_by_subject[subj].add(value)
                return
            # object property carried as a plain string (currentLocation)
            if name in OBJECT_PROPERTIES:
                if name == "hasLocation":
                    self.pending_locations.append((subj, value.strip()))
                return
            # plain datatype property
            self.g.add((subj, URIRef(iri),
                        to_literal(value, el.get("valueType", "xs:string"))))

        elif EXPORT_DPP_URN_PROPS and iri and iri.startswith(DPP_URN_PREFIX):
            pred = DATA_NS[iri[len(DPP_URN_PREFIX):]]
            self.g.add((subj, pred,
                        to_literal(value, el.get("valueType", "xs:string"))))

    # -- pass 3: finishing touches ----------------------------------------
    def emit_types(self):
        for subj, types in self.types_by_subject.items():
            for t in types:
                self.g.add((subj, RDF.type, SC[t]))

    def emit_site_has_resource(self):
        for resource, loc in self.pending_locations:
            if not self.types_by_subject.get(resource, set()) & RESOURCE_CLASSES:
                continue
            key = LOCATION_ALIASES.get(loc, loc).strip().lower()
            site = self.site_by_name.get(key)
            if site is None:
                print(f"[warn] currentLocation '{loc}' of {resource} "
                      f"matches no Site (add it to LOCATION_ALIASES)",
                      file=sys.stderr)
                continue
            self.g.add((site, SITE_HAS_RESOURCE, resource))

    def emit_property_declarations(self):
        """Declare dataset-level properties, mirroring scadient:suppliesTo
        in the reference dataset."""
        used = {p for _, p, _ in self.g}
        if DATA_NS.suppliesTo in used:
            self.g.add((DATA_NS.suppliesTo, RDF.type, OWL.ObjectProperty))
            self.g.add((DATA_NS.suppliesTo, RDFS.label, Literal("supplies to")))
            self.g.add((DATA_NS.suppliesTo, RDFS.comment,
                        Literal("Direct supply/delivery flow from one site to "
                                "another (from Neo4j [:TO] relationship)")))
            self.g.add((DATA_NS.suppliesTo, RDFS.domain, SC.Site))
            self.g.add((DATA_NS.suppliesTo, RDFS.range, SC.Site))
        if SITE_HAS_RESOURCE in used:
            self.g.add((SITE_HAS_RESOURCE, RDF.type, OWL.ObjectProperty))
            self.g.add((SITE_HAS_RESOURCE, RDFS.label, Literal("site has resource")))
            self.g.add((SITE_HAS_RESOURCE, RDFS.comment,
                        Literal("Links a Site to the (container) Resources "
                                "currently located at it, derived from the "
                                "DPP currentLocation field")))
            self.g.add((SITE_HAS_RESOURCE, RDFS.domain, SC.Site))
            self.g.add((SITE_HAS_RESOURCE, RDFS.range, SC.Resource))


# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--basyx", metavar="URL",
                     help="BaSyx AAS Environment base URL, e.g. http://localhost:8081")
    src.add_argument("--files", nargs="+", metavar="GLOB",
                     help="AAS environment JSON files (offline mode)")
    ap.add_argument("-o", "--output", default="msc_dataset.ttl")
    args = ap.parse_args()

    if args.basyx:
        shells, submodels = load_from_basyx(args.basyx)
    else:
        shells, submodels = load_from_files(args.files)

    print(f"Loaded {len(shells)} shells, {len(submodels)} submodels")
    g = Exporter(shells, submodels).export()
    g.serialize(destination=args.output, format="turtle")
    print(f"Wrote {len(g)} triples to {args.output}")


if __name__ == "__main__":
    main()
