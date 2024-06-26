import logging
from typing import TYPE_CHECKING, Any, Dict, List, TypedDict, Union

from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.watch import ChangeType, DocumentChange
from proto.datetime_helpers import DatetimeWithNanoseconds
from semver import Version, compare

# This is done to get type hints and avoid circular imports.
# Is not necessary, but it helps with type hints.
if TYPE_CHECKING:
    from firestore_device_services_handler import FirestoreDeviceServicesHandler

logger = logging.getLogger(__name__)


class InstalledService(TypedDict):
    """Dict representing the installed_services structure in firebase."""

    version: str
    env: Dict[str, Any]


class FirestoreServiceUpdateHandler:
    """Handle the updates of a service by listening to the service versions on firestore and setting the installed service as updatable when the latest version is newer that the installed version."""

    STOP_DELAY_TIME_S = 0.5

    def __init__(
        self,
        installed_service_document: DocumentSnapshot,
        device_handler: "FirestoreDeviceServicesHandler",
    ):
        logger.debug("New service added to installed_services.")

        self.name = installed_service_document.id
        self.versions: List[str] = []

        fields_dict = installed_service_document.to_dict()
        self.installed_fields = InstalledService(**fields_dict) if fields_dict else None

        self._installed_version = (
            self.installed_fields.get("version") if self.installed_fields else None
        )
        self._latest_version: Union[str, None] = None

        self.device_handler = device_handler
        self.client = device_handler.client
        self.subscription = None

    @property
    def installed_version(self):
        """Service version installed on the device."""
        return self._installed_version

    @installed_version.setter
    def installed_version(self, version: str):
        self._installed_version = version
        self._refresh_updatable_services()

    @property
    def latest_version(self):
        """Latest service version available on the marketplace."""
        return self._latest_version

    @latest_version.setter
    def latest_version(self, version: str):
        self._latest_version = version
        self._refresh_updatable_services()

    def start(self):
        """Start the handler by listening to the services on firestore."""
        self.subscription = (
            self.client.collection("services")
            .document(self.name)
            .collection("versions")
            .on_snapshot(self._on_version_change)
        )
        logger.info(f"{self.name} Update Handler started.")

    def stop(self):
        """Stop the handler by stopping the listener."""
        logger.info(f"{self.name} Update Handler stopped.")
        if self.subscription is not None:
            self.subscription.unsubscribe()

    def restart(self):
        """Restart the handler. This function just calls to `start()` and `stop()`."""
        self.stop()
        self.start()

    def update_fields(self, installed_service_document: DocumentSnapshot):
        """Given the firestore document snapshot of the installed service, update the services `installed_fields`."""
        self.name = installed_service_document.id

        document_dict = installed_service_document.to_dict()
        if document_dict is not None:
            self.installed_fields = InstalledService(**document_dict)
            self.installed_version = self.installed_fields.get("version")

    def _refresh_updatable_services(self):
        """Refresh the updatable services of the `device_handler` by comparing `installed_version` and `latest_version`."""
        if self.latest_version is None or self.installed_version is None:
            return

        if compare(self.latest_version, self.installed_version) == 1:
            self.device_handler.set_as_updatable(self.name, self.latest_version)
        else:
            self.device_handler.set_as_updated(self.name)

    def _update_latest_version(self):
        """Recompute the `latest_version` by comparing the `versions`."""
        self.latest_version = max(self.versions, key=Version.parse)

    def _on_version_change(
        self,
        snapshots: List[DocumentSnapshot],
        changes: List[DocumentChange],
        read_time: DatetimeWithNanoseconds,
    ):
        """When there is a change on the service versions, update the `versions` and update the latest version.

        This function is an `on_snapshot()` function for the "installed_services" collection of each "device" document.
        This function activates when a change occurs on the collection.
        """

        for change in changes:
            if change.type == ChangeType.ADDED:
                logger.debug(f"A new version of {self.name} service was added.")
                new_version = change.document.id
                if not new_version in self.versions:
                    self.versions.append(change.document.id)

            elif change.type == ChangeType.REMOVED:
                logger.debug(f"A version of {self.name} service was removed.")
                self.versions.remove(change.document.id)

            self._update_latest_version()
