import pika
import uuid
import json
import os
import sys
import argparse
import yaml
import torch

import src.Log
from src.RpcClient import RpcClient
from src.Scheduler import Scheduler

parser = argparse.ArgumentParser(description="Split learning framework")
parser.add_argument('--layer_id', type=int, required=True, help='ID of layer, start from 1')
parser.add_argument('--device', type=str, required=False, help='Device of client')
parser.add_argument('--client_id', type=int, required=False,
                    help='ID of client; defaults to "client_id" in setup.json')
args = parser.parse_args()

# ── Per-machine identity ─────────────────────────────────────────────────────
# setup.json holds THIS device's client_id, so a client is started with only its
# stage:  python client.py --layer_id 1
# It is deliberately not in config.yaml: that file is the RUN's configuration and
# is the same on every machine (guide/README invariant 9), while the id is the one
# value that must differ per device — it names this client's reply queue
# (reply_<client_id>) and is how the server registers it. Two clients sharing an id
# share a reply queue and the run silently misroutes.
# --client_id still wins when given, for running several clients on one machine.
SETUP_PATH = 'setup.json'


def load_client_id(cli_client_id):
    if cli_client_id is not None:
        return cli_client_id
    if not os.path.exists(SETUP_PATH):
        sys.exit(f"[!] No --client_id given and no {SETUP_PATH} in {os.getcwd()}.\n"
                 f"    Create it with:  {{\"client_id\": 1}}")
    try:
        with open(SETUP_PATH, 'r') as f:
            setup = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        sys.exit(f"[!] Cannot read {SETUP_PATH}: {e}")
    if setup.get("client_id") is None:
        sys.exit(f"[!] {SETUP_PATH} has no \"client_id\" field.")
    try:
        return int(setup["client_id"])
    except (TypeError, ValueError):
        sys.exit(f"[!] {SETUP_PATH}: client_id must be an integer, "
                 f"got {setup['client_id']!r}")


client_id = load_client_id(args.client_id)
print(f"Using client_id: {client_id} "
      f"({'--client_id' if args.client_id is not None else SETUP_PATH})")

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# client_id = uuid.uuid4()
address = config["rabbit"]["address"]
username = config["rabbit"]["username"]
password = config["rabbit"]["password"]
virtual_host = config["rabbit"]["virtual-host"]

device = None

if args.device is None:
    if torch.cuda.is_available():
        device = "cuda"
        print(f"Using device: {torch.cuda.get_device_name(device)}")
    else:
        device = "cpu"
        print(f"Using device: CPU")
else:
    device = args.device
    print(f"Using device: {device}")

logger = src.Log.Logger(f"./app.log" , config['debug-mode'])
logger.log_info(f"Application start.")

credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(pika.ConnectionParameters(address, 5672, f'{virtual_host}', credentials))
channel = connection.channel()

if __name__ == "__main__":
    src.Log.print_with_color("[>>>] Client sending registration message to server...", "red")
    data = {"action": "REGISTER", "client_id": client_id, "layer_id": args.layer_id, "message": "Hello from Client!"}
    scheduler = Scheduler(client_id, args.layer_id, channel, device)
    logger.log_debug(f"client_id : {client_id} , stage {args.layer_id} , "
                     f"channel {channel} , device {device}")
    client = RpcClient(client_id, args.layer_id, channel ,logger ,scheduler.inference_func, device,
                       configure_func=scheduler.configure_measurements)
    client.send_to_server(data)
    client.wait_response()
