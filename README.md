# VirtualICE Configuration

This repository contains a portable and containerized version of the data collection architecture of the ICE Laboratory.
It is designed to be used with Docker and Docker Compose, allowing for easy setup and deployment of the VirtualICE system.

## Prerequisites

- Docker: Ensure you have Docker installed on your machine. You can download it from [Docker
Install](https://docs.docker.com/get-docker/).
- Docker Compose: This is typically included with Docker Desktop, but you can also install it separately if needed. Check the [Docker Compose installation guide](https://docs.docker.com/compose/install/).
- WSL2 (for Windows users): If you are using Windows, you need to have WSL2 installed and configured. Follow the instructions on the [WSL installation guide](https://docs.microsoft.com/en-us/windows/wsl/install).

## Quick start: the line moving in Unity

On any PC with Docker (Windows: Docker Desktop, nothing else):

1. Clone with the submodules:
   ```bash
   git clone -b dev --recurse-submodules https://github.com/esd-univr/virtualice-image.git
   ```
2. Put a `.env` file in the repository root with the Kafka/RabbitMQ credentials used by ICE Teach
   (ask for it, never commit it; an `.envrc` works as is, renamed):
   ```
   KAFKA_CLIENT_USER=...
   KAFKA_CLIENT_PASSWORD=...
   RABBITMQ_USERNAME=...
   RABBITMQ_PASSWORD=...
   ```
3. Start the stack and the Frost line (the first time it builds the images, several minutes):
   ```bash
   docker compose up -d
   ```
4. Open ICE Teach (Unity Editor: Play, or the build). The `frost-ext` container waits for it on Kafka, then
   runs the line for 20 minutes.

Unity restarted, or the 20 minutes are over: `docker compose restart frost-ext`.
Logs: `docker compose logs -f frost-ext`. Stop everything: `docker compose down`.

## Getting Started

**Note**: Run all commands below in a Linux or macOS terminal with Docker support (on Windows, use WSL2).

1. **Clone the Repository**:
   Open your terminal and run the following command to clone the repository:
   ```bash
   git clone https://github.com/esd-univr/virtualice-image.git --recurse-submodules
   ```
2. **Add the .envrc File**:
   Create a `.envrc` file in the root directory of the cloned repository. 
   This file is used to set environment variables for the Docker containers. 
   You can use the provided example as a template:
   
   ```bash
   touch .envrc
   echo 'export KAFKA_CLIENT_USER="your_username"' >> .envrc
   echo 'export KAFKA_CLIENT_PASSWORD="your_password"' >> .envrc
   echo 'export RABBITMQ_USERNAME="your_username"' >> .envrc
   echo 'export RABBITMQ_PASSWORD="your_password"' >> .envrc
   ```

   Once you have created the `.envrc` file, you can use `direnv` to load the environment variables automatically. 
   If you don't have `direnv` installed, you can install it from [direnv.net](https://direnv.net/).

    After installing `direnv`, run the following command to allow the environment variables:
    
    ```bash
    direnv allow
    ```
    
    Alternatively, you can manually export the variables in your terminal session:
    
    ```bash
    source .envrc
    ```

3. **Start the Docker Containers**:
    ```bash
    docker compose --env-file .envrc up -d
    ```
    `kafka-init` creates the FrostTwin topics and exits; `frost-ext` runs the Frost line once ICE Teach is
    connected (Quick start above). Compose (>= 2.23.1) writes the Kafka client configuration from the
    credentials: `generate_client_config.sh` is needed only by `docker-compose-player.yml`.

4. **Access the Services**:
    - **Kafka**: Access the Kafka service at `localhost:9093` from your local machine. Inside the Docker network, you can use the service name `kafka-0:9092`, `kafka-1:9092`, or `kafka-2:9092` to connect to the Kafka brokers.
    - **RabbitMQ**: AMQP at `localhost:30690`, management interface at `http://localhost:15672`, with the credentials of your `.envrc` file.


5. **Send Messages to Kafka**:
   You can send spot messages to Kafka using the provided `ice_player/send_kafka_msg.py` script.

   ```bash
   python ice_player/send_kafka_msg.py --bootstrap-servers localhost:9093 --topic your_topic_name --msgs "message1" "message2"
   ```

   Alternatively, you can use the `ice_player/player.py` script to replay messages from a recording file:

   ```bash
   python ice_player/player.py --path your_recording.pkl --simulate-delays --bootstrap-servers localhost:9093 --topics list_of_topics_to_republish
   ```

   For more details on how to use these scripts use the `--help` option:

   ```bash
   python ice_player/send_kafka_msg.py --help
   python ice_player/player.py --help
   ```

**Note**: If you encounter any issues with the services not starting correctly, or if you change the docker-compose configuration, you may need to clean the local directory by running:

```bash
# stop and remove all containers if they are running
docker-compose down 

# remove all containers, networks, and volumes
chmod +x clean_env.sh
./clean_env.sh

# and then start again
docker-compose up -d
```
## Kafka contract: the same messages as the real line

What this stack publishes on Kafka must be a subset of what the real ICE line publishes, in the same
form (topic, measurement, tags, field names and types). The contract is built from the raw Kafka
recordings of the line (ICE_DIR/kafka-record/record/raw) and stored in `kafka/contract.json`:

```bash
python3 utils/kafka_contract.py catalog ~/ICE_DIR/kafka-record/record/raw/*.jsonl   # rebuild kafka/contract.json
python3 utils/kafka_contract.py check messages.jsonl                               # must report 0 outside
```

`utils/kafka_contract.py` also builds, for an OPC UA variable, the line the real line publishes for it
(`contract_line`). Consumers (e.g. Unity) adapt to these messages, never the other way round.

`ros_bridge/` holds the formats of the RB-Kairos ROS bridge (pose and UR arm, straight to Kafka on the
real line, not through OPC UA), the arm motion recorded per mission and the end-of-mission poses: see
[ros_bridge/README.md](ros_bridge/README.md).

## ICE Teach digital twin

`ice_teach/` runs the ICE Teach Unity build against this stack: see [ice_teach/README.md](ice_teach/README.md).
