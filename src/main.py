import logging
import signal

from communication_module import CommunicationModule
from default_firestore_client import DefaultFirestoreClient
from device_handler import DeviceHandler

LOG_LEVEL = "INFO"

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s - %(name)-16s - %(message)s", level=LOG_LEVEL
)
logger = logging.getLogger(__name__)

def signal_handler(sig, frame):
    comm_module.stop()
    client.stop_client()
    device_handler.stop()

if __name__ == "__main__":
    comm_module = CommunicationModule(host="127.0.0.1", port=5555)
    comm_module.start()

    api_key = str(comm_module.execute_command("get_api_key"))
    project_id = str(comm_module.execute_command("get_project_id"))
    refresh_token = str(comm_module.execute_command("get_refresh_token"))
    device_id = str(comm_module.execute_command("get_device_id"))

    if not (api_key and project_id and refresh_token and device_id):
        exit("Service wasn't able to get the necessary information from the config file handler")

    client = DefaultFirestoreClient(api_key, project_id, refresh_token)
    client.initialize_client()

    if client.client is None or client.user_id is None:
        exit("Not valid client or user_id")

    device_handler = DeviceHandler(client.client, client.user_id, device_id)
    device_handler.start()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.pause()
