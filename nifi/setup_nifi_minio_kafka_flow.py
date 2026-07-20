#!/usr/bin/env python3
"""Configure NiFi pour lire MinIO/S3 et publier les donnees Secur-SN vers Kafka."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


MINIO_GROUP_NAME = "Secur_SN_Ingestion_MinIO_Kafka"
LOCAL_GROUP_NAME = "Secur_SN_Ingestion_Local"
KAFKA_GROUP_NAME = "Secur_SN_Ingestion_Kafka"
DEFAULT_BOOTSTRAP = "kafka:19092"
DEFAULT_BUCKET = "secur-sn-landing"
DEFAULT_ENDPOINT = "http://minio:9000"
DEFAULT_REGION = "us-east-1"
DEFAULT_ACCESS_KEY = "securadmin"
DEFAULT_SECRET_KEY = "securadmin123"
DEFAULT_CREDENTIALS_SERVICE_NAME = "Secur SN MinIO Credentials"
MINIO_GROUP_POSITION = {"x": 40.0, "y": 40.0}
LEGACY_GROUP_POSITIONS = {
    KAFKA_GROUP_NAME: {"x": 760.0, "y": 80.0},
    LOCAL_GROUP_NAME: {"x": 760.0, "y": 400.0},
}
S3_CREDENTIALS_PROPERTY = "AWS Credentials Provider service"
S3_LEGACY_CREDENTIALS_PROPERTY = "AWS Credentials Provider Service"
S3_PREFIX_PROPERTY = "prefix"
S3_LEGACY_PREFIX_PROPERTY = "Prefix"
CREDENTIALS_ACCESS_KEY_PROPERTY = "Access Key"
CREDENTIALS_SECRET_KEY_PROPERTY = "Secret Key"
CREDENTIALS_LEGACY_ACCESS_KEY_PROPERTY = "Access Key ID"
CREDENTIALS_LEGACY_SECRET_KEY_PROPERTY = "Secret Access Key"


class NifiClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"NiFi API {method} {path} -> HTTP {exc.code}: {error_body}"
            ) from exc

    def wait_until_ready(self, timeout_seconds: int = 180) -> None:
        deadline = time.time() + timeout_seconds
        last_error = ""
        while time.time() < deadline:
            try:
                self.request("GET", "/flow/about")
                return
            except Exception as exc:
                last_error = str(exc)
                time.sleep(5)
        raise RuntimeError(f"NiFi indisponible apres {timeout_seconds}s: {last_error}")

    def get_root_group_id(self) -> str:
        flow = self.request("GET", "/flow/process-groups/root")
        return flow["processGroupFlow"]["id"]

    def get_group_flow(self, group_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]

    def find_child_group(self, parent_id: str, name: str) -> Optional[str]:
        for group in self.get_group_flow(parent_id).get("processGroups", []):
            component = group.get("component", {})
            if component.get("name") == name:
                return component.get("id") or group.get("id")
        return None

    def find_bundle(self, processor_type: str) -> Dict[str, Any]:
        types = self.request("GET", "/flow/processor-types").get("processorTypes", [])
        for processor in types:
            if processor.get("type") == processor_type:
                return processor["bundle"]
        raise RuntimeError(f"Type processor NiFi introuvable: {processor_type}")

    def create_process_group(self, parent_id: str, name: str) -> str:
        entity = self.request(
            "POST",
            f"/process-groups/{parent_id}/process-groups",
            {
                "revision": {"version": 0},
                "component": {"name": name, "position": MINIO_GROUP_POSITION},
            },
        )
        return entity["component"]["id"]

    def get_process_group(self, group_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/process-groups/{group_id}")

    def move_process_group(self, group_id: str, position: Dict[str, float]) -> None:
        entity = self.get_process_group(group_id)
        component = entity["component"]
        current_position = component.get("position", {})
        if (
            current_position.get("x") == position["x"]
            and current_position.get("y") == position["y"]
        ):
            return
        self.request(
            "PUT",
            f"/process-groups/{group_id}",
            {
                "revision": entity["revision"],
                "component": {
                    "id": component["id"],
                    "name": component["name"],
                    "position": position,
                },
                "disconnectedNodeAcknowledged": False,
            },
        )

    def create_processor(
        self,
        group_id: str,
        name: str,
        processor_type: str,
        x: float,
        y: float,
        properties: Dict[str, str],
        auto_terminated: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/process-groups/{group_id}/processors",
            {
                "revision": {"version": 0},
                "component": {
                    "type": processor_type,
                    "bundle": self.find_bundle(processor_type),
                    "name": name,
                    "position": {"x": x, "y": y},
                    "config": {
                        "properties": properties,
                        "autoTerminatedRelationships": auto_terminated or [],
                    },
                },
            },
        )

    def connect(
        self,
        group_id: str,
        name: str,
        source_id: str,
        destination_id: str,
        relationships: List[str],
    ) -> None:
        self.request(
            "POST",
            f"/process-groups/{group_id}/connections",
            {
                "revision": {"version": 0},
                "component": {
                    "name": name,
                    "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
                    "destination": {"id": destination_id, "groupId": group_id, "type": "PROCESSOR"},
                    "selectedRelationships": relationships,
                    "flowFileExpiration": "0 sec",
                    "backPressureObjectThreshold": 10000,
                    "backPressureDataSizeThreshold": "1 GB",
                },
            },
        )

    def set_processor_state(self, entity: Dict[str, Any], state: str) -> None:
        self.request(
            "PUT",
            f"/processors/{entity['component']['id']}/run-status",
            {
                "revision": entity["revision"],
                "state": state,
                "disconnectedNodeAcknowledged": False,
            },
        )

    def update_processor_properties(
        self,
        entity: Dict[str, Any],
        properties: Dict[str, str],
        auto_terminated: Optional[List[str]] = None,
        remove_properties: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        component = entity["component"]
        current_config = component.get("config", {})
        current_properties = current_config.get("properties", {})
        merged_properties = dict(current_properties)
        merged_properties.update(properties)
        for property_name in remove_properties or []:
            merged_properties[property_name] = None
        config: Dict[str, Any] = {"properties": merged_properties}
        if auto_terminated is not None:
            config["autoTerminatedRelationships"] = auto_terminated
        return self.request(
            "PUT",
            f"/processors/{component['id']}",
            {
                "revision": entity["revision"],
                "component": {"id": component["id"], "config": config},
                "disconnectedNodeAcknowledged": False,
            },
        )

    def get_group_status(self, group_id: str) -> Dict[str, Any]:
        response = self.request("GET", f"/flow/process-groups/{group_id}/status")
        return response["processGroupStatus"]["aggregateSnapshot"]

    def terminate_processor_threads(self, processor_id: str) -> None:
        self.request("DELETE", f"/processors/{processor_id}/threads")

    def stop_group_processors(self, group_id: str, timeout_seconds: int = 60) -> None:
        """Arrete le groupe avant toute reconfiguration de ses processeurs."""
        for processor in self.get_group_flow(group_id).get("processors", []):
            component = processor.get("component", {})
            if component.get("state") != "STOPPED":
                self.set_processor_state(processor, "STOPPED")

        deadline = time.monotonic() + timeout_seconds
        terminated_threads = False
        while time.monotonic() < deadline:
            processors = self.get_group_flow(group_id).get("processors", [])
            status = self.get_group_status(group_id)
            active_threads = int(status.get("activeThreadCount", 0))
            processors_stopped = all(
                processor.get("component", {}).get("state") == "STOPPED"
                for processor in processors
            )
            if processors_stopped and active_threads == 0:
                return

            # An ancien PublishKafka peut garder un thread bloque apres un changement de cluster.
            if processors_stopped and active_threads > 0 and not terminated_threads:
                for processor in processors:
                    self.terminate_processor_threads(processor["component"]["id"])
                terminated_threads = True
            time.sleep(1)
        raise RuntimeError(f"Arret des processeurs NiFi incomplet pour le groupe {group_id}")


def publish_properties(topic: str, bootstrap: str) -> Dict[str, str]:
    return {
        "bootstrap.servers": bootstrap,
        "topic": topic,
        "use-transactions": "true",
        "message-demarcator": "\n",
        "acks": "all",
        "Failure Strategy": "Route to Failure",
        "security.protocol": "PLAINTEXT",
        "attribute-name-regex": "secur\\..*",
        "compression.type": "none",
        "ack.wait.time": "30 sec",
        "max.block.ms": "30 sec",
    }


def list_s3_properties(
    bucket: str,
    endpoint: str,
    region: str,
    s3_prefix: str,
    credentials_service_id: str,
) -> Dict[str, str]:
    return {
        S3_CREDENTIALS_PROPERTY: credentials_service_id,
        "Bucket": bucket,
        "Endpoint Override URL": endpoint,
        S3_PREFIX_PROPERTY: s3_prefix,
        "Region": region,
    }


def fetch_s3_properties(
    bucket: str,
    endpoint: str,
    region: str,
    credentials_service_id: str,
) -> Dict[str, str]:
    return {
        S3_CREDENTIALS_PROPERTY: credentials_service_id,
        "Bucket": bucket,
        "Endpoint Override URL": endpoint,
        "Object Key": "${filename}",
        "Region": region,
    }


class MinioNifiClient(NifiClient):
    """Ajoute les operations NiFi necessaires aux controller services S3."""

    def find_controller_bundle(self, service_type: str) -> Dict[str, Any]:
        service_types = self.request("GET", "/flow/controller-service-types").get("controllerServiceTypes", [])
        for service in service_types:
            if service.get("type") == service_type:
                return service["bundle"]
        raise RuntimeError(f"Type controller service NiFi introuvable: {service_type}")

    def get_controller_service(self, service_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/controller-services/{service_id}")

    def list_controller_services(self, group_id: str) -> List[Dict[str, Any]]:
        return self.request("GET", f"/flow/process-groups/{group_id}/controller-services").get(
            "controllerServices",
            [],
        )

    def find_controller_services(self, group_id: str, name: str) -> List[Dict[str, Any]]:
        matches: List[Dict[str, Any]] = []
        for service in self.list_controller_services(group_id):
            component = service.get("component", {})
            if component.get("name") == name:
                matches.append(service)
        return matches

    def create_controller_service(
        self,
        group_id: str,
        name: str,
        service_type: str,
        properties: Dict[str, str],
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/process-groups/{group_id}/controller-services",
            {
                "revision": {"version": 0},
                "component": {
                    "type": service_type,
                    "bundle": self.find_controller_bundle(service_type),
                    "name": name,
                    "properties": properties,
                },
            },
        )

    def set_controller_service_state(self, entity: Dict[str, Any], state: str) -> Dict[str, Any]:
        component = entity["component"]
        return self.request(
            "PUT",
            f"/controller-services/{component['id']}/run-status",
            {
                "revision": entity["revision"],
                "state": state,
                "disconnectedNodeAcknowledged": False,
            },
        )

    def wait_controller_service_state(
        self,
        service_id: str,
        expected_state: str,
        timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        deadline = time.time() + timeout_seconds
        last_entity: Dict[str, Any] = {}
        while time.time() < deadline:
            last_entity = self.get_controller_service(service_id)
            component = last_entity.get("component", {})
            if component.get("state") == expected_state:
                return last_entity
            time.sleep(1)
        component = last_entity.get("component", {})
        raise RuntimeError(
            f"Controller service {service_id} attendu {expected_state}, "
            f"etat actuel {component.get('state')}, erreurs: {component.get('validationErrors')}"
        )

    def disable_controller_service(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        component = entity["component"]
        if component.get("state") == "DISABLED":
            return entity
        self.set_controller_service_state(entity, "DISABLED")
        return self.wait_controller_service_state(component["id"], "DISABLED")

    def update_controller_service_properties(
        self,
        entity: Dict[str, Any],
        properties: Dict[str, str],
        remove_properties: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        component = entity["component"]
        merged_properties = dict(component.get("properties", {}))
        merged_properties.update(properties)
        for property_name in remove_properties or []:
            merged_properties[property_name] = None
        return self.request(
            "PUT",
            f"/controller-services/{component['id']}",
            {
                "revision": entity["revision"],
                "component": {"id": component["id"], "properties": merged_properties},
                "disconnectedNodeAcknowledged": False,
            },
        )

    def enable_controller_service(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        component = entity["component"]
        if component.get("state") != "ENABLED":
            self.set_controller_service_state(entity, "ENABLED")
        enabled = self.wait_controller_service_state(component["id"], "ENABLED")
        errors = enabled.get("component", {}).get("validationErrors") or []
        if errors:
            raise RuntimeError(f"Controller service S3 invalide: {errors}")
        return enabled


def credentials_properties(access_key: str, secret_key: str) -> Dict[str, str]:
    return {
        CREDENTIALS_ACCESS_KEY_PROPERTY: access_key,
        CREDENTIALS_SECRET_KEY_PROPERTY: secret_key,
    }


def create_or_get_credentials_service(
    client: MinioNifiClient,
    group_id: str,
    access_key: str,
    secret_key: str,
) -> str:
    existing_services = client.find_controller_services(group_id, DEFAULT_CREDENTIALS_SERVICE_NAME)
    if existing_services:
        selected = existing_services[0]
        for service in existing_services:
            disabled = client.disable_controller_service(service)
            updated = client.update_controller_service_properties(
                disabled,
                credentials_properties(access_key, secret_key),
                remove_properties=[
                    CREDENTIALS_LEGACY_ACCESS_KEY_PROPERTY,
                    CREDENTIALS_LEGACY_SECRET_KEY_PROPERTY,
                ],
            )
            if service["component"]["id"] == selected["component"]["id"]:
                selected = updated
        enabled = client.enable_controller_service(selected)
        return enabled["component"]["id"]

    service = client.create_controller_service(
        group_id,
        DEFAULT_CREDENTIALS_SERVICE_NAME,
        "org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
        credentials_properties(access_key, secret_key),
    )
    enabled = client.enable_controller_service(service)
    return enabled["component"]["id"]


def create_minio_channel(
    client: MinioNifiClient,
    group_id: str,
    prefix: str,
    s3_prefix: str,
    topic: str,
    quarantine_dir: str,
    y: float,
    bootstrap: str,
    bucket: str,
    endpoint: str,
    region: str,
    credentials_service_id: str,
) -> List[Dict[str, Any]]:
    list_s3 = client.create_processor(
        group_id,
        f"{prefix} ListS3",
        "org.apache.nifi.processors.aws.s3.ListS3",
        0,
        y,
        list_s3_properties(bucket, endpoint, region, s3_prefix, credentials_service_id),
    )
    fetch_s3 = client.create_processor(
        group_id,
        f"{prefix} FetchS3Object",
        "org.apache.nifi.processors.aws.s3.FetchS3Object",
        360,
        y,
        fetch_s3_properties(bucket, endpoint, region, credentials_service_id),
    )
    tag = client.create_processor(
        group_id,
        f"{prefix} Tag source",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        720,
        y,
        {
            "secur.flow": MINIO_GROUP_NAME,
            "secur.source": "minio",
            "secur.stage": "minio_nifi_kafka_published",
            "secur.topic": topic,
        },
    )
    publish = client.create_processor(
        group_id,
        f"{prefix} PublishKafka",
        "org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        1080,
        y,
        publish_properties(topic, bootstrap),
        auto_terminated=["success"],
    )
    quarantine = client.create_processor(
        group_id,
        f"{prefix} Quarantine",
        "org.apache.nifi.processors.standard.PutFile",
        1440,
        y + 130,
        {
            "Conflict Resolution Strategy": "replace",
            "Create Missing Directories": "true",
            "Directory": quarantine_dir,
        },
        auto_terminated=["success", "failure"],
    )

    client.connect(group_id, f"{prefix} objects -> fetch", list_s3["component"]["id"], fetch_s3["component"]["id"], ["success"])
    client.connect(group_id, f"{prefix} fetch -> tag", fetch_s3["component"]["id"], tag["component"]["id"], ["success"])
    client.connect(group_id, f"{prefix} tag -> kafka", tag["component"]["id"], publish["component"]["id"], ["success"])
    client.connect(
        group_id,
        f"{prefix} fetch failure -> quarantine",
        fetch_s3["component"]["id"],
        quarantine["component"]["id"],
        ["failure"],
    )
    client.connect(
        group_id,
        f"{prefix} kafka failure -> quarantine",
        publish["component"]["id"],
        quarantine["component"]["id"],
        ["failure"],
    )
    return [quarantine, publish, tag, fetch_s3, list_s3]


def start_processors(client: MinioNifiClient, processors: List[Dict[str, Any]]) -> None:
    for processor in processors:
        if processor.get("component", {}).get("state") == "STOPPED":
            client.set_processor_state(processor, "RUNNING")


def wait_for_valid_processors(
    client: MinioNifiClient,
    group_id: str,
    timeout_seconds: int = 60,
) -> List[Dict[str, Any]]:
    deadline = time.time() + timeout_seconds
    last_invalid: List[str] = []
    while time.time() < deadline:
        processors = client.get_group_flow(group_id).get("processors", [])
        invalid: List[str] = []
        for processor in processors:
            component = processor.get("component", {})
            if component.get("validationStatus") == "INVALID":
                errors = "; ".join(component.get("validationErrors") or [])
                invalid.append(f"{component.get('name')}: {errors}")
        if not invalid:
            return processors
        last_invalid = invalid
        time.sleep(2)
    raise RuntimeError("Processeurs NiFi invalides: " + " | ".join(last_invalid))


def create_flow(
    client: MinioNifiClient,
    bootstrap: str,
    bucket: str,
    endpoint: str,
    region: str,
    access_key: str,
    secret_key: str,
) -> str:
    root_id = client.get_root_group_id()

    for legacy_group_name in (LOCAL_GROUP_NAME, KAFKA_GROUP_NAME):
        legacy_group_id = client.find_child_group(root_id, legacy_group_name)
        if legacy_group_id:
            client.stop_group_processors(legacy_group_id)
            client.move_process_group(legacy_group_id, LEGACY_GROUP_POSITIONS[legacy_group_name])

    existing_group_id = client.find_child_group(root_id, MINIO_GROUP_NAME)
    if existing_group_id:
        client.move_process_group(existing_group_id, MINIO_GROUP_POSITION)
        client.stop_group_processors(existing_group_id)
        credentials = create_or_get_credentials_service(client, existing_group_id, access_key, secret_key)
        processors = client.get_group_flow(existing_group_id).get("processors", [])
        for processor in processors:
            name = processor.get("component", {}).get("name", "")
            if name == "01 Incidents ListS3":
                client.update_processor_properties(
                    processor,
                    list_s3_properties(bucket, endpoint, region, "incidents/", credentials),
                    remove_properties=[S3_LEGACY_CREDENTIALS_PROPERTY, S3_LEGACY_PREFIX_PROPERTY],
                )
            elif name == "02 Meteo ListS3":
                client.update_processor_properties(
                    processor,
                    list_s3_properties(bucket, endpoint, region, "meteo/", credentials),
                    remove_properties=[S3_LEGACY_CREDENTIALS_PROPERTY, S3_LEGACY_PREFIX_PROPERTY],
                )
            elif name == "01 Incidents FetchS3Object":
                client.update_processor_properties(
                    processor,
                    fetch_s3_properties(bucket, endpoint, region, credentials),
                    remove_properties=[S3_LEGACY_CREDENTIALS_PROPERTY],
                )
            elif name == "02 Meteo FetchS3Object":
                client.update_processor_properties(
                    processor,
                    fetch_s3_properties(bucket, endpoint, region, credentials),
                    remove_properties=[S3_LEGACY_CREDENTIALS_PROPERTY],
                )
            elif name == "01 Incidents PublishKafka":
                client.update_processor_properties(
                    processor,
                    publish_properties("secur_incidents_raw", bootstrap),
                    auto_terminated=["success"],
                )
            elif name == "02 Meteo PublishKafka":
                client.update_processor_properties(
                    processor,
                    publish_properties("secur_meteo", bootstrap),
                    auto_terminated=["success"],
                )
        start_processors(client, wait_for_valid_processors(client, existing_group_id))
        print(f"Controller service S3 actif: {credentials}")
        return existing_group_id

    group_id = client.create_process_group(root_id, MINIO_GROUP_NAME)
    credentials_service_id = create_or_get_credentials_service(client, group_id, access_key, secret_key)
    processors: List[Dict[str, Any]] = []
    processors.extend(
        create_minio_channel(
            client,
            group_id,
            "01 Incidents",
            "incidents/",
            "secur_incidents_raw",
            "/opt/nifi/nifi-current/data/secur-sn/quarantine/minio/incidents",
            0,
            bootstrap,
            bucket,
            endpoint,
            region,
            credentials_service_id,
        )
    )
    processors.extend(
        create_minio_channel(
            client,
            group_id,
            "02 Meteo",
            "meteo/",
            "secur_meteo",
            "/opt/nifi/nifi-current/data/secur-sn/quarantine/minio/meteo",
            360,
            bootstrap,
            bucket,
            endpoint,
            region,
            credentials_service_id,
        )
    )

    start_processors(client, wait_for_valid_processors(client, group_id))
    return group_id


def main() -> int:
    parser = argparse.ArgumentParser(description="Configure NiFi comme producteur Kafka depuis MinIO/S3.")
    parser.add_argument("--url", default="http://localhost:8081/nifi-api")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--access-key", default=DEFAULT_ACCESS_KEY)
    parser.add_argument("--secret-key", default=DEFAULT_SECRET_KEY)
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()

    client = MinioNifiClient(args.url)
    client.wait_until_ready(timeout_seconds=args.timeout)
    group_id = create_flow(
        client,
        args.bootstrap,
        args.bucket,
        args.endpoint,
        args.region,
        args.access_key,
        args.secret_key,
    )
    print(f"Flow NiFi MinIO Kafka pret: {MINIO_GROUP_NAME} ({group_id})")
    print(f"MinIO bucket: {args.bucket}")
    print(f"MinIO endpoint NiFi: {args.endpoint}")
    print(f"Kafka brokers: {args.bootstrap}")
    print("Topics: secur_incidents_raw, secur_meteo")
    print("Interface: http://localhost:8081/nifi")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
