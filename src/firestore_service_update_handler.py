import logging
from typing import TYPE_CHECKING, List, Union

from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.watch import ChangeType, DocumentChange
from proto.datetime_helpers import DatetimeWithNanoseconds
from semver import Version, compare

# This is done to get type hints and avoid circular imports.
# Is not necessary, but it helps with type hints.
if TYPE_CHECKING:
    from firestore_device_services_handler import FirestoreDeviceServicesHandler

logger = logging.getLogger(__name__)


class FirestoreServiceUpdateHandler:
    """Handles the posible version updates of a service 

    This is done by comparing the installed version to the marketplace versions on Firestore.
    If the marketplace latest version is newer than the installed on, the service is set as updatable.
    """

    def __init__(
        self,
        service_name: str,
        device_handler: "FirestoreDeviceServicesHandler",
    ):
        logger.debug(f"'{service_name}' Service Update Handler created.")

        self.service_name = service_name
        self.device_handler = device_handler
        self.installed_version = None
        self.marketplace_versions: List[str] = []
        self.marketplace_latest_version: Union[str, None] = None
        self.client = device_handler.client
        self.subscription = None

    def start(self):
        """Start the handler by listening to the marketplace service versions on firestore."""
        logger.debug(f"Starting '{self.service_name}' Service Update Handler.")
        self.subscription = (
            self.client.collection("services")
            .document(self.service_name)
            .collection("versions")
            .on_snapshot(self._on_version_change)
        )

    def stop(self):
        """Stop the handler by stopping the listener."""
        logger.debug(f"Stopping '{self.service_name}' Service Update Handler.")
        if self.subscription is not None:
            self.subscription.unsubscribe()

    def restart(self):
        """Restart the handler. This function just calls to `start()` and `stop()`."""
        self.stop()
        self.start()

    def update_installed_version(self, new_version: str):
        """Update the installed version of the service."""
        logger.debug(
            f"'{self.service_name}' service installed version updated to {new_version}.")
        self.installed_version = new_version
        self._refresh_updatable_services()

    def _update_markeplace_latest_version(self):
        """Recompute the `latest_version` by comparing the `versions`."""
        self.marketplace_latest_version = max(
            self.marketplace_versions, key=Version.parse)
        self._refresh_updatable_services()

    def _refresh_updatable_services(self):
        """Refresh the updatable services of the `device_handler` by comparing `installed_version` and `latest_version`."""
        if self.marketplace_latest_version is None or self.installed_version is None:
            return

        if compare(self.marketplace_latest_version, self.installed_version) == 1:
            self.device_handler.set_as_updatable(
                self.service_name, self.marketplace_latest_version)
        else:
            self.device_handler.set_as_updated(self.service_name)

    def _on_version_change(
        self,
        snapshots: List[DocumentSnapshot],
        changes: List[DocumentChange],
        read_time: DatetimeWithNanoseconds,
    ):
        """When there is a change on the service versions, update the `versions` and update the latest version.

        This function is an `on_snapshot()` function for the "versions" collection of each markeplace services.
        This function activates when a change occurs on the collection.
        """

        for change in changes:
            version = change.document.id

            if change.type == ChangeType.ADDED or change.type == ChangeType.MODIFIED:
                logger.debug(
                    f"A new version of '{self.service_name}' service has been added: {version}.")
                if not version in self.marketplace_versions:
                    self.marketplace_versions.append(version)

            elif change.type == ChangeType.REMOVED:
                logger.debug(
                    f"A version of '{self.service_name}' service has been removed: {version}.")
                self.marketplace_versions.remove(version)

            self._update_markeplace_latest_version()
