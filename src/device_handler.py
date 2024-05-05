import logging
from typing import Dict, List, Union

from google.cloud.firestore_v1 import Client, DocumentReference, DocumentSnapshot, Watch
from google.cloud.firestore_v1.watch import ChangeType, DocumentChange
from proto.datetime_helpers import DatetimeWithNanoseconds

from service_handler import ServiceHandler

logger = logging.getLogger(__name__)


class DeviceHandler:
    """Handler of the firestore device.

    This class handles the installed services and updatable services of a device.
    It creates a `ServiceHandler` for each installed service.
    """

    client: Client
    """Firestore database client."""
    device: DocumentReference
    """Reference to the firestore document of this device."""
    installed_services: List[ServiceHandler]
    """List of the installed services on the device."""
    updatable_services: Dict[str, str]
    """List of the updatable services of the device."""
    watch: Union[Watch, None]
    """The `watch` of the `on_snapshot` function."""

    def __init__(self, client: Client, user_id: str, device_id: str) -> None:
        self.client = client
        self.device = (
            client.collection("users")
            .document(user_id)
            .collection("devices")
            .document(device_id)
        )
        self.installed_services = []

        document_dict = self.device.get().to_dict()
        if document_dict is None or document_dict.get("updatable_services") is None:
            self.updatable_services = {}
        else:
            updatable_services_dict = document_dict.get("updatable_services")
            if updatable_services_dict is None:
                self.updatable_services = {}
            else:
                self.updatable_services = updatable_services_dict

        self.watch = None

    def start(self):
        """Start the handler by listening to the installed services of the firestore device."""
        logger.info("Device Handler started.")
        self.watch = self.device.collection("installed_services").on_snapshot(
            self._on_new_installed_service
        )

    def stop(self):
        logger.info("Device Handler stopped.")
        """Stop the handler by stopping the listener."""
        if self.watch is not None:
            self.watch.unsubscribe()

    def restart(self):
        """Restart the handler. This function just calls to `start()` and `stop()`."""
        self.start()
        self.stop()

    def set_as_updated(self, service: str):
        """Set the given installed service as updated by removing it from the updatable services."""
        if self.updatable_services.get(service) is not None:
            del self.updatable_services[service]
            self.device.set(
                {"updatable_services": self.updatable_services},
                merge=True,
            )

    def set_as_updatable(self, service: str, version: str):
        """Set the given installed service and version updatable by adding it to the updatable services."""
        self.updatable_services[service] = version
        self.device.set(
            {"updatable_services": self.updatable_services},
            merge=True,
        )

    def _get_service_by_name(self, service_name: str):
        """Get the installed service with the given name."""
        for service in self.installed_services:
            if service.name == service_name:
                return service

    def _remove_service_by_name(self, service_name: str):
        """Remove the installed service with the given name."""
        for index, service in enumerate(self.installed_services):
            if service.name == service_name:
                service.stop()
                del self.installed_services[index]
                break

    def _on_new_installed_service(
        self,
        snapshots: List[DocumentSnapshot],
        changes: List[DocumentChange],
        read_time: DatetimeWithNanoseconds,
    ):
        """When there is a change on the installed services, add, remove or modify the service.

        There are three types of changes: ADDED, REMOVED and MODIFIED.
        - ADDED: Create a `ServiceHandler`, add it to `services` and start the `ServiceHandler`.
        - REMOVED: Stop the `ServiceHanlder` and remove it from `services`.
        - MODIFIED: Update the fields of the `ServiceHandler` and restart it.

        This function is an `on_snapshot()` function for the "installed_services" collection of each "device" document.
        This function activates when a change occurs on the collection.
        """

        for change in changes:
            installed_service: DocumentSnapshot = change.document
            service_name = installed_service.id

            if change.type == ChangeType.ADDED:
                logger.debug(f"New {service_name} service installed on device.")
                service = ServiceHandler(self.client, installed_service, self)
                self.installed_services.append(service)
                service.start()

            elif change.type == ChangeType.REMOVED:
                logger.debug(f"{service_name} service removed from device.")
                service = self._get_service_by_name(service_name)
                if service:
                    service.stop()
                self._remove_service_by_name(service_name)
                self.set_as_updated(service_name)

            elif change.type == ChangeType.MODIFIED:
                logger.debug(f"Changes on {service_name} service on device.")
                service = self._get_service_by_name(service_name)
                self._remove_service_by_name(service_name)
                if service:
                    service.update_fields(installed_service)
                    service.restart()
                    self.installed_services.append(service)
