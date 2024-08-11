import logging
import threading
from typing import List, Dict

from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.watch import ChangeType, DocumentChange
from proto.datetime_helpers import DatetimeWithNanoseconds

from firestore_client_handler import FirestoreClientHandler
from firestore_service_update_handler import FirestoreServiceUpdateHandler

logger = logging.getLogger(__name__)


class FirestoreDeviceServicesHandler(FirestoreClientHandler):
    """Handler which listens to the installed services of a device and starts the corresponding `FirestoreServiceUpdateHandler` for each service."""

    RESTART_DELAY_TIME_S = 0.5

    def __init__(
        self,
        api_key: str,
        project_id: str,
        refresh_token: str,
        device_id: str,
    ):
        super().__init__(api_key, project_id, refresh_token)
        self.device_id = device_id
        self.installed_services: List[FirestoreServiceUpdateHandler] = []
        self.updatable_services: Dict[str, str] = {}
        self.device = None
        self.subscription = None

    def start(self):
        """Start the firestore client and the listener of the installed services."""
        logger.debug("Starting Firestore Device Services Handler.")
        self.initialize_client()

    def stop(self):
        """Stop the handler by stopping the listener and the firestore client."""
        logger.debug("Stopping Firestore Device Services Handler.")
        for service in self.installed_services:
            threading.Thread(target=service.stop).start()
        self.installed_services = []

        if self.subscription is not None:
            self.subscription.unsubscribe()
            self.subscription = None

        self.stop_client()

    def restart(self):
        """Restart the handler. This function just calls to `stop()` and `start()`."""
        self.stop()
        self.start()

    def set_as_updated(self, service_name: str):
        """Set the given installed service as updated by removing it from the updatable services."""
        if service_name not in self.updatable_services:
            return
        logger.info(f"Setting the '{service_name}' service as updated.")
        del self.updatable_services[service_name]
        self.device.update({"updatable_services": self.updatable_services})

    def set_as_updatable(self, service_name: str, version: str):
        """Set the given installed service and version updatable by adding it to the updatable services."""
        if self.updatable_services.get(service_name) == version:
            return
        logger.info(f"Setting the '{service_name}' service as updatable to version '{version}'.")
        self.updatable_services[service_name] = version
        self.device.update({"updatable_services": self.updatable_services})

    def on_client_initialized(self):
        """Callback function when the client is initialized."""
        logger.debug("Firestore client initialized")
        self.device = (
            self.client.collection("users")
            .document(self.user_id)
            .collection("devices")
            .document(self.device_id)
        )
        self._sync_updatable_services()

        self.subscription = self.device.collection("installed_services").on_snapshot(
            self._on_new_installed_service
        )

    def on_server_not_responding(self):
        """Callback function when the server is not responding."""
        logger.error("Firestore server not responding")
        threading.Timer(self.RESTART_DELAY_TIME_S, self.restart).start()

    def on_token_expired(self):
        """Callback function when the token is expired."""
        logger.debug("Refreshing Firebase client token id")
        threading.Timer(self.RESTART_DELAY_TIME_S, self.restart).start()

    def _sync_updatable_services(self):
        """Sync the updatable_services in Firebase with the local updatable_services."""
        logger.debug("Syncing updatable services in Firebase locally.")
        if not self.device:
            return
        document_dict = self.device.get().to_dict()
        if document_dict:
            self.updatable_services = document_dict.get("updatable_services", {})
        else:
            self.updatable_services = {}

    def _get_service_by_name(self, service_name: str):
        """Get the installed service with the given name."""
        for service in self.installed_services:
            if service.service_name == service_name:
                return service

    def _remove_service_by_name(self, service_name: str):
        """Remove the installed service with the given name."""
        for index, service in enumerate(self.installed_services):
            if service.service_name == service_name:
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
            installed_service_doc: DocumentSnapshot = change.document
            service_name = installed_service_doc.id
            service_version = installed_service_doc.to_dict().get("version")

            if change.type == ChangeType.ADDED:
                logger.debug(f"'{service_name}' ({service_version}) service installed notification received.")
                service = FirestoreServiceUpdateHandler(service_name, self)
                self.installed_services.append(service)
                service.update_installed_version(service_version)
                service.start()

            elif change.type == ChangeType.REMOVED:
                logger.debug(f"'{service_name}' service removed notification received.")
                self._remove_service_by_name(service_name)
                self.set_as_updated(service_name)

            elif change.type == ChangeType.MODIFIED:
                logger.debug(f"'{service_name}' ({service_version}) service updated notification received.")
                service = self._get_service_by_name(service_name)
                if service:
                    service.update_installed_version(service_version)
