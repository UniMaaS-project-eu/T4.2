# UniMaaS Unified Naming Conventions v2.0

## Overview

This document defines the **unified naming convention** for Digital Product Passports (DPP) in UniMaaS, ensuring consistent and traceable identifiers across:
- **AAS (Asset Administration Shell)** representation
- **RDF (Resource Description Framework)** export following MSC Ontology
- **All four pilots** (Adient, ANV, Aegean, Catone)

---

## Core Principle: Semantic URN Structure

### AAS Shell ID Format

```
urn:unimaas:<pilot>:<MSC_entity_type>:<idShort>
│   │       │        │                  │
│   │       │        │                  └─ Local identifier (alphanumeric + underscores)
│   │       │        └────────────────────── MSC class name (lowercase in URN)
│   │       └──────────────────────────────── Pilot identifier (adient, anv, aegean, catone)
│   └────────────────────────────────────────── UniMaaS project identifier
└──────────────────────────────────────────────── URN scheme
```

### RDF Subject IRI Format

```
<pilot_namespace>:<MSC_prefix>_<idShort>
│                 │            │
│                 │            └─── Local identifier (from AAS idShort)
│                 └─────────────────── MSC class prefix (see mapping table)
└──────────────────────────────────────── Pilot data namespace
```

---

## Naming Examples

### Example 1: Product (Returnable Container)

**AAS Shell:**
```json
{
  "id": "urn:unimaas:adient:product:magnum_optimum_1114567",
  "idShort": "magnum_optimum_1114567",
  "assetInformation": {
    "globalAssetId": "urn:asset:adient:product:magnum_optimum_1114567"
  }
}
```

**RDF Subject:**
```turtle
scadient:product_magnum_optimum_1114567 a sc:Product ;
    sc:partNumber "MO-1114567"^^xsd:string ;
    sc:manufacturingDate "2024-01-15"^^xsd:date .
```

### Example 2: Site (Manufacturing Facility)

**AAS Shell:**
```json
{
  "id": "urn:unimaas:adient:site:site_33",
  "idShort": "site_33",
  "assetInformation": {
    "globalAssetId": "urn:asset:adient:site:33"
  }
}
```

**RDF Subject:**
```turtle
scadient:site_site_33 a sc:Site ;
    sc:nodeId 33 ;
    sc:siteType "JIT"^^xsd:string ;
    sc:country "HU"^^xsd:string .
```

### Example 3: LogisticRoute (Transportation Route)

**AAS Shell:**
```json
{
  "id": "urn:unimaas:adient:logisticroute:route_11_33",
  "idShort": "route_11_33",
  "assetInformation": {
    "globalAssetId": "urn:asset:adient:route:11_33"
  }
}
```

**RDF Subject:**
```turtle
scadient:route_route_11_33 a sc:LogisticRoute ;
    sc:hasStartingPoint scadient:site_site_11 ;
    sc:hasEndingPoint scadient:site_site_33 ;
    sc:transportDistance 2185 ;
    sc:freightMode "FTL"^^xsd:string .
```

---

## MSC Entity Type → RDF Prefix Mapping

| MSC Entity Type | URN (lowercase) | RDF Prefix | Example RDF Subject |
|---|---|---|---|
| Product | product | product | scadient:product_magnum_optimum_1114567 |
| Site | site | site | scadient:site_site_33 |
| Location | location | location | scadient:location_warehouse_madrid |
| LogisticRoute | logisticroute | route | scadient:route_11_33 |
| Process | process | process | scadient:process_manufacturing |
| ProcessConfiguration | processconfiguration | pconf | scadient:pconf_config_1 |
| Resource | resource | resource | scadient:resource_resource_1 |
| MaterialResource | materialresource | material | scadient:material_steel_plate |
| HumanResource | humanresource | human | scadient:human_technician_1 |
| SoftwareResource | softwareresource | software | scadient:software_erp_system |
| EquipmentResource | equipmentresource | equipment | scadient:equipment_cnc_mill |
| ResourceConf | resourceconf | rconf | scadient:rconf_config_1 |
| Supplier | supplier | supplier | scadient:supplier_acme_corp |
| Sensor | sensor | sensor | scadient:sensor_temp_sensor_1 |
| Device | device | device | scadient:device_iot_gateway_1 |
| Characteristic | characteristic | char | scadient:char_weight |
| CharacteristicType | characteristictype | ctype | scadient:ctype_mass_type |
| CustomerRequirement | customerrequirement | creq | scadient:creq_eco_requirement |

---

## Naming Rules

### 1. Identifier Style

**DO:**
- Use lowercase alphanumeric characters
- Use underscores `_` for word separation
- Use consistent naming across AAS and RDF

**DON'T:**
- Use hyphens `-` (convert to underscores)
- Use spaces
- Use CamelCase (convert to lowercase)
- Use special characters except underscores

### 2. Examples

**Conversions:**

| Original | Normalized | Reason |
|---|---|---|
| B-Container-3334567 | b_container_3334567 | Hyphens → underscores, lowercase |
| Europallet 2224567 | europallet_2224567 | Space → underscore |
| MagnumOptimum1114567 | magnum_optimum_1114567 | CamelCase → lowercase_underscore |
| Site #33 | site_33 | Remove special chars |
| Product_ABC | product_abc | Lowercase |

### 3. idShort Field

The AAS `idShort` field **must match** the last component of the `id` URN.

```json
{
  "id": "urn:unimaas:adient:product:magnum_optimum_1114567",
  "idShort": "magnum_optimum_1114567",  //  Matches last component
  
  //  WRONG:
  // "idShort": "MagnumOptimum1114567",  // Different naming style
  // "idShort": "magnum-optimum",  // Doesn't match, uses hyphens
}
```

---

## Pilot Namespaces

### URN Prefixes

```
Adient:   urn:unimaas:adient:<type>:<id>
ANV:      urn:unimaas:anv:<type>:<id>
Aegean:   urn:unimaas:aegean:<type>:<id>
Catone:   urn:unimaas:catone:<type>:<id>
```

### RDF Namespaces

```
Adient:   http://unimaas-project.eu/MSCOntology/data/adient/
ANV:      http://unimaas-project.eu/MSCOntology/data/anv/
Aegean:   http://unimaas-project.eu/MSCOntology/data/aegean/
Catone:   http://unimaas-project.eu/MSCOntology/data/catone/
```

### RDF Prefix Bindings

```turtle
@prefix sc: <http://unimaas-project.eu/MSCOntology#> .
@prefix scadient: <http://unimaas-project.eu/MSCOntology/data/adient/> .
@prefix scanv: <http://unimaas-project.eu/MSCOntology/data/anv/> .
@prefix scaegean: <http://unimaas-project.eu/MSCOntology/data/aegean/> .
@prefix sccatone: <http://unimaas-project.eu/MSCOntology/data/catone/> .
```

---

## AAS Global Asset ID Format

The `globalAssetId` provides additional semantic context:

```
urn:asset:<pilot>:<entity_type>:<identifier>
```

**Examples:**

```json
// Product
"globalAssetId": "urn:asset:adient:product:magnum_optimum_1114567"

// Site
"globalAssetId": "urn:asset:adient:site:33"

// Route
"globalAssetId": "urn:asset:adient:route:11_33"
```

---

## Submodel ID Format

Submodels follow a hierarchical structure:

```
urn:aas:<pilot>:<entity_type>:<entity_id>:sm:<submodel_name>
```

**Examples:**

```json
// Digital Nameplate for product
"id": "urn:aas:adient:product:magnum_optimum_1114567:sm:nameplate"

// Carbon Footprint for product
"id": "urn:aas:adient:product:magnum_optimum_1114567:sm:carbonfootprint"

// Product Lifecycle
"id": "urn:aas:adient:product:magnum_optimum_1114567:sm:lifecycle"
```
---
## Submodel SemanticIds

For the RDF exporter to incorporate all (datatype and object) properties included in the AAS structure, each AAS property should have a valid SemanticId of the type "http://unimaas-project.eu/MSCOntology#"

```json
// example for the numberOfUses MSC Datatype Property
"modelType": "Property",
"idShort": "numberOfUses",
"valueType": "xs:integer",
"value": "0",
"semanticId": {
  "type": "ExternalReference",
  "keys": [
    {
      "type": "GlobalReference",
      "value": "http://unimaas-project.eu/MSCOntology#numberOfUses"
    }
  ]
}
```
---

## Property SemanticIds

### Data Properties

Data properties link to MSC Ontology concepts via their `semanticId`:

```json
{
  "modelType": "Property",
  "idShort": "transportDistance",
  "value": 2185,
  "valueType": "xs:integer",
  "semanticId": {
    "type": "ExternalReference",
    "keys": [{
      "type": "GlobalReference",
      "value": "http://unimaas-project.eu/MSCOntology#transportDistance"
    }]
  }
}
```

**RDF Result:**
```turtle
scadient:route_11_33 sc:transportDistance 2185 .
```

### Object Properties

Object properties reference other entities:

```json
{
  "modelType": "ReferenceElement",
  "idShort": "hasStartingPoint",
  "semanticId": {
    "keys": [{
      "value": "http://unimaas-project.eu/MSCOntology#hasStartingPoint"
    }]
  },
  "value": {
    "keys": [{
      "type": "GlobalReference",
      "value": "urn:unimaas:adient:site:site_11"
    }]
  }
}
```

**RDF Result:**
```turtle
scadient:route_11_33 sc:hasStartingPoint scadient:site_site_11 .
```

---



## Validation Checklist

When creating AAS shells, verify:

- [ ] AAS `id` follows format: `urn:unimaas:<pilot>:<type>:<idshort>`
- [ ] MSC entity type in URN is **lowercase** (e.g., `logisticroute`, not `LogisticRoute`)
- [ ] `idShort` matches the last component of `id`
- [ ] All identifiers use **underscores only** (no hyphens)
- [ ] All identifiers are **lowercase**
- [ ] `globalAssetId` provides semantic context
- [ ] All property `semanticId` values point to MSC Ontology
- [ ] Object property `value` contains reference to valid AAS `id`

---

## MSC Ontology Integration

All identifiers are traceable to MSC Ontology classes:

```turtle
# MSC Ontology class definition
sc:Product a owl:Class ;
    rdfs:comment "Physical or digital product in supply chain" .

sc:Site a owl:Class ;
    rdfs:comment "Manufacturing facility or warehouse location" .

sc:LogisticRoute a owl:Class ;
    rdfs:comment "Transportation route between two sites" .

# Data properties
sc:transportDistance a owl:DatatypeProperty ;
    rdfs:domain sc:LogisticRoute ;
    rdfs:range xsd:integer .

# Object properties
sc:hasStartingPoint a owl:ObjectProperty ;
    rdfs:domain sc:LogisticRoute ;
    rdfs:range sc:Site .
```

---

## Tools & Validation


### RDF Generation

The exporter automatically converts:
```
AAS ID:          urn:unimaas:adient:product:magnum_optimum_1114567
Pilot namespace: http://unimaas-project.eu/MSCOntology/data/adient/
MSC prefix:      product
RDF subject:     scadient:product_magnum_optimum_1114567
```

---

## Summary

| Aspect | Format | Example |
|---|---|---|
| **AAS Shell ID** | urn:unimaas:<pilot>:<type>:<id> | urn:unimaas:adient:product:magnum_optimum_1114567 |
| **AAS idShort** | lowercase_underscore | magnum_optimum_1114567 |
| **RDF Subject** | <ns>:<prefix>_<id> | scadient:product_magnum_optimum_1114567 |
| **Naming Style** | lowercase + underscores | only_underscores_no_hyphens |
| **Entity Types** | MSC class names | Product, Site, LogisticRoute |
| **Pilots** | 4 URN prefixes | adient, anv, aegean, catone |

---
