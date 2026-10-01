# VO-Security

# Device Setup Requirements

In the scenario where a **Virtual Object (VO)** consumes data from a **device**, the device must be configured correctly so that its properties are available and can be discovered and accessed by the VO.

This section describes the minimum required configuration for the `device` component.

## Required security components

    - Verifier (Mandatory)
    - PEP-Proxy (Mandatory)

## 1. `app.py`

The device application must expose its properties through `exposed_thing`.

This is required so that the device properties are available through the WoT runtime and can be consumed by the VO.

`app.py` must:

- create the Thing
- define the properties that the device exposes
- expose those properties using `exposed_thing`
- start the Thing so it becomes reachable

If a property is not added to the exposed thing, the VO will not be able to access it.

---

## 2. `config.yaml`

The device configuration must use these ports:

- **Catalog port:** `9091`
- **Properties port:** `8081`

Required configuration:

```yaml
catalog:
  port: 9091

properties:
  port: 8081
```

## 3. `td.json`

The `td.json` file defines the Thing Description of the device and specifies the properties that the VO can consume.

This file must:

- include a `description`
- configure bearer security
- declare the exposed properties

Example format:

```json
{
    "title": "device",
    "id": "urn:dev:wot:test",
    "description": "Test.",
    "securityDefinitions": {
        "bearer_sc": {
            "scheme": "bearer"
        }
    },
    "security": "bearer_sc",
    "@context": [
        "https://www.w3.org/2022/wot/td/v1.1"
    ],
    "properties": {
        "temperature": {
            "type": "integer"
        },
        "humidity": {
            "type": "integer"
        }
    }
}
```

# VO Setup Requirements

In the scenario where a **Virtual Object (VO)** consumes data from a **device**, the VO must also be configured correctly so that it can expose its own Thing and interact with the device.

This section describes the minimum required configuration for the `vo` component.

## Required security components

    - Holder (Mandatory)
    - PEP-Proxy (Just needed if VO has properties that are consumed by other VOs/cVOs)
    - Verifier (Just needed if VO has properties that are consumed by other VOs/cVOs)

## 1. `app.py`

Same configuration as in the device.

The VO application must:

- create the Thing
- define the properties that the VO exposes
- expose those properties using `exposed_thing`
- start the Thing so it becomes reachable

If a property is not added to the exposed thing, it will not be available to other components.

---

## 2. `config.yaml`

Same configuration as in the device.

Additionally, the following section must be included under `securitySB`:

```yaml
securitySB:
  securitySBHTTP:
    securityScheme: oidc4vp
    holderUrl: "http://vo-holder:8085"
    requester: vo
```

Where:

- `securityScheme` must be set to `oidc4vp`
- `holderUrl` is the address and port of the VO Holder service
- `requester` is the name/id of the VO

## 3. `td.json`

Same configuration as in the device.

The `td.json` file must:

- include the WoT context
- define a Thing `title`
- define a Thing `id`
- include a `description`
- configure bearer security
- declare the exposed properties

The property names defined in `td.json` must match the properties exposed in `app.py`.

# Docker Compose Configuration

The `docker-compose.yaml` file must define the services required for the device side and the VO side.

## Verifier

This service runs the verifier logic.

It must:

- use the image `dockerhub.odins.es/unimaas/oidc4vp:latest` (to be updated with UNIMAAS Reference)
- run with the command `["verifier"]`
- run with the following environment variables:

Example:
```yaml
device-verifier:
  image: dockerhub.odins.es/unimaas/oidc4vp:latest
  container_name: device-verifier
  command: ["verifier"]
  environment:
    logs: "false"
    xacml: "true"
    xacml_domain: unimaas
    xacml_pdp: https://unimaas.odins.es/pdp/verdict
    blockchain_rest: https://unimaas.odins.es
  restart: unless-stopped

```
Environment variables:

- `logs`: enables or disbales logs. Default value `false`
- `xacml`: enables XACML Authorization. Default value `true`
- `xacml_domain`: XACML domain name,
- `xacml_pdp`: PDP endpoint used for authorization decisions
- `blockchain_rest`: blockchain REST endpoint

## Holder

This service runs the Holder.

It must:

- use the image `dockerhub.odins.es/unimaas/oidc4vp:latest` (to be updated with UNIMAAS reference)
- run with the command `["holder"]`
- define the blockchain and issuer endpoints

Example:
```yaml
vo-holder:
  image: dockerhub.odins.es/unimaas/oidc4vp:latest
  container_name: vo-holder
  command: ["holder"]
  environment:
    logs: "false"
    blockchain_rest: https://unimaas.odins.es
    issuer_host: https://unimaas.odins.es/issuer
  restart: unless-stopped
```

Environment variables:

- `logs`: enables or disables logs. Default value `false`
- `blockchain_rest`: blockchain REST endpoint
- `issuer_host`: issuer endpoint

## PEP-Proxy

This service runs the PEP-PRoxy exposed to the outside

It must:

- use the image `dockerhub.odins.es/unimaas/pep-proxy:latest` (to be updated with UNIMAAS reference)
- expose ports 8080 and 9090
- set correspondat environment variables

Example:

```yaml
device-proxy:
    image: dockerhub.odins.es/unimaas/pep-proxy:latest
    container_name: device-proxy
    networks:
      device:
      edge:
        aliases:
          - device
    ports:
      - "8080:8080"
      - "9090:9090"
    environment:
      # BASIC SETTINGS
      pep_protocol: "http"
      PEP_ENDPOINT: "http://device:8080,http://device:9090"
      oidc4vp_verifier_host: "device-verifier"
      oidc4vp_verifier_port: "8090"
      target_host: "device"
      target_port: "8081"
      target2_host: "device"
      target2_port: "9091"
      target2_thingdescription: "/device"
    depends_on:
      - device-internal
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s
    restart: unless-stopped
```

Environment variables:

- `pep_protocol`: protocol used by the proxy. Default value `http`
- `oidc4vp_verifier_host`: verifier host name. This value MUST be the same as the verifier service
- `oidc4vp_verifier_port`: verifier port. Default value `8090`
- `target_hots`: internal target host for properties
- `target_port`: internal target port for properties. Default value `8081`
- `target2_host`: internal target host for catalog
- `target2_port`: internal target port for catalog. Default value `9091
- `target2_thingdescription`: Thing Description path
- `PEP_ENDPOINT`: exposed proxy endpoints

# Creating XACML Policies using the PAP Web Interface

This section explains how to create attributes, rules, and policies using the XACML **Policy Administration Point (PAP)** web interface.

These policies are required to authorize access from a **Virtual Object (VO)** to a protected device resource (for example, a Thing Description).

---

## 1. Login to the PAP Interface

Open the PAP web interface (https://unimaas.odins.es/pap/) and log in using valid credentials.

Credentials must be requested from: **aaraias@odins.es**

---

## 2. (Optional) Create a New Domain

After login, the PAP main page is displayed.

To create a new domain:

1. Click **Create new domain**
2. Enter the domain name
3. Click **Submit**

Domain names:

- must be a single string
- must not contain spaces
- must not contain special characters

Example: unimaas

The registration process may take a few seconds because the domain is stored on the blockchain.

After completion, verify that the domain appears in the **Select domain** dropdown list.

Domains are used to separate policy sets between different use cases (for example, one domain per pilot deployment).

---

## 3. Select the Domain and Open Attribute Management

If the domain already exists:

1. Select it from **Select domain**
2. Click **Manage attributes**

This opens the **Attributes Management** view.

Attributes define the authorization triplets used in policies: reousrce + action + subject

Where:

- **resource** = target URL to be accessed (for example a Thing Description endpoint)
- **action** = HTTP method (GET, POST, PUT, etc.)
- **subject** = requester identifier (VO, CVO, or application)

Example:
- **resource**: http:/device:9090/device
- **action**: GET
- **subject**: vo

---

# Registering Attributes

Before creating policies, attributes must be registered.

---

## 4. Register a Resource

Inside **Attributes Management**:

1. Click **New Resource**
2. Enter the resource URL

Example: http://device:9090/device


Do not modify: Type Resource ID


Click: OK


---

## 5. Register an Action

Click: New Action


Enter the HTTP method in uppercase.

Example: GET


Important:

The method **must be uppercase**

Do not modify: Type Action ID

Click: OK


---

## 6. Register a Subject

Click: New Subject

Fill both fields: Type Subject Name and Type Subject ID


Important:

The subject name **must match the requester field defined in the VO config.yaml**

Example:

```yaml
securitySB:
  securitySBHTTP:
    securityScheme: oidc4vp
    holderUrl: "http://vo-holder:8085"
    requester: vo
```

Example: vo

For Type Subject ID: Select the option that ends with 'id'

Click OK

## 7. Save attributes

After creating the attribute triplet, **click on 'Save All Attributes'**

If successful, the following message will appear: 'Operation Carried Out Successfully'

To verify correct registration:

1. Return to the main page.
2. Reopen 'Manage Attributes'
3. If the attributes registered appear, it is confirmed that the registration was successful.

---

# Creating Policies

## 8. Open Policy Management

Once attributes are registered, policies can be created.

From the PAP main page:

Click: Manage Policies

The policy editor view opens.

Registered attributes will appear.

---

## XACML Structure Overview

XACML authorization follows this hierarchy:

Domain
  - Policies
    - Rules
      - Attribute triplets


Where:

- a **domain** groups policies for a use case
- a **policy** groups multiple rules
- a **rule** defines a decision (**Permit** or **Deny**)
- each rule matches an attribute triplet: resource + action + subject

---

## 9. Create a Policy

Inside the **Policies** section:

Click: New

Enter a policy name.

Example: test

Policy names:

- are only identifiers
- should not contain spaces
- should not contain special characters

A policy contains one or more rules. Policies are grouped inside a domain. Domains are used to separate policy sets between different deployments or use cases.

---

## 10. Create a Rule

Select the policy first.

Then inside **Rules**:

Click: New

Enter a rule name.

Example: deviceTD

Rule names are only identifiers for easier management.

Click: OK

---

## 11. Assign Attributes to the Rule

Select the created rule.

Then:

1. Uncheck 'All' in all attributes.
2. Select the attributes to form the triplet.

Example:

- resource: http://device:9090/device
- subject: vo
- action: GET

3. Set rule effect: Permit
4. Click 'Apply'

If successful, the following message will appear: 'Operation Carried Out Successfully'

The rule created indicates that the Virtual Object 'vo' is authorized to access the resource http://device:9090/device using the HTTP method 'GET'.

To verify correct registration:

Return to the main page and reopen **Manage Policies**

---

## 12. PDP Synchronization Delay

After creating a rule, it may take a few seconds before the PDP updates.

During this time, the authorization decision may not yet be applied.










