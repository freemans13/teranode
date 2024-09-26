# Clean_env.sh 

## Overview

This script is designed to perform several operations on a Kubernetes cluster, including:

- Cleaning Aerospike namespaces.
- Backing up and truncating PostgreSQL databases.
- Clearing Kafka data.
- Scaling down and up all relevant services using the k8s operator

## Prerequisites

Before running this script, ensure that the following tools are installed:

- **kubectl**: Kubernetes command-line tool. Install using `brew install kubectl`.
- **aws-cli**: AWS Command Line Interface. Install using `brew install awscli`.

## Usage

### Setting Up

- The script should be run with the `KUBECONFIG` environment variable unset. The script will exit if `KUBECONFIG` is set.

### Running the Script

1. The script begins by scaling down all relevant services using the `down.sh` script.
2. It then performs cleanup operations across multiple regions and namespaces in the background:
   - **Aerospike Cleaning**: Truncates specific Aerospike namespaces.
   - **PostgreSQL Truncation**: Drops specific tables from PostgreSQL databases.
   - **Kafka Clearing**: Clears Kafka data.
3. Once the cleanup operations are complete, the script waits for all background processes to finish.
4. Finally, it scales up all relevant services using the `up.sh` script.

### Handling Interrupts

- The script includes a trap to handle `SIGINT` (Ctrl+C), which will stop all background processes gracefully.

## Functions

### `clean()`

- **Purpose**: Cleans specific Aerospike namespaces.
- **Parameters**:
  - `region`: AWS region.
  - `namespace`: Kubernetes namespace.

### `backup()`

- **Purpose**: Backs up PostgreSQL databases and uploads the backup to S3.
- **Parameters**:
  - `region`: AWS region.
  - `namespace`: Kubernetes namespace.

### `truncate()`

- **Purpose**: Truncates specific PostgreSQL tables.
- **Parameters**:
  - `region`: AWS region.
  - `namespace`: Kubernetes namespace.

### `clear_kafka()`

- **Purpose**: Clears Kafka data using a custom Kafka tool.
- **Parameters**:
  - `namespace`: Kubernetes namespace.

## Background Processes

- The script runs cleanup operations (`clean`, `truncate`, `clear_kafka`) in parallel using background processes. It captures their process IDs and waits for all of them to finish before proceeding.

## Example

```bash
#!/bin/bash
set -ex

# Script execution example
./clean_env.sh
```

# Down.sh
# Script Documentation: Kubernetes Scale Down

## Overview

This script is designed to scale down Kubernetes clusters based on specified parameters. It handles different scaling operations depending on the input arguments.

## Usage

### Syntax

```bash
./clean_env.sh [all|t1|t2|t3]
```

### Arguments

- **`all`**: Scales down all regions.
- **`t1`**: Scales down the `eu-central-1` region with suffix `1`.
- **`t2`**: Scales down the `eu-central-1` region with suffix `2`.
- **`t3`**: Scales down the `eu-central-1` region with suffix `3`.
- **`help`**: Displays usage information.

### Example

```bash
./down.sh all
./down.sh t1
```

## Script Details

### Help Option

- If the first argument is `help`, the script prints usage instructions and exits.

### `scale_down` Function

- **Purpose**: Deletes Kubernetes clusters in a specified namespace.
- **Parameters**:
  - `region`: AWS region.
  - `namespace_suffix`: Namespace suffix.

```bash
scale_down() {
  local region=$1
  local namespace_suffix=$2

  kubectl delete clusters.teranode.bsvblockchain.org -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground --all
}
```

### Main Execution Flow

1. **Check for Help Argument**: If `help` is passed as the first argument, display usage instructions and exit.

2. **Scale Down Operations**:
   - **If `all` is passed**: Scales down all regions (`t1`, `t2`, `t3`) concurrently.
   - **If `t1`, `t2`, or `t3` is passed**: Scales down the specified region and namespace concurrently.
   - **Unknown Arguments**: Prints an error message and exits.

3. **Wait for Background Processes**: The script waits for all background processes to complete before exiting.

### Background Processes

- The script runs scale-down operations in parallel using background processes. Process IDs are collected and waited on to ensure completion.

# Up.sh
# Script Documentation: Kubernetes Scale Up

## Overview

This script is designed to scale up Kubernetes clusters based on specified parameters. It handles different scaling operations depending on the input arguments.

## Usage

### Syntax

```bash
./up.sh [all|t1|t2|t3]
```

### Arguments

- **`all`**: Scales up all regions.
- **`t1`**: Scales up the `eu-central-1` region with suffix `1`.
- **`t2`**: Scales up the `eu-central-1` region with suffix `2`.
- **`t3`**: Scales up the `eu-central-1` region with suffix `3`.
- **`help`**: Displays usage information.

### Example

```bash
./up.sh all
./up.sh t1
```

## Script Details

### Help Option

- If the first argument is `help`, the script prints usage instructions and exits.

### `scale_up` Function

- **Purpose**: Applies Kubernetes configurations to scale up clusters.
- **Parameters**:
  - `region`: AWS region.
  - `namespace_suffix`: Namespace suffix.

```bash
scale_up() {
  local region=$1
  local namespace_suffix=$2
  two_layers_up=$(dirname "$(realpath "$0")")/../../..
  kubectl apply -f $two_layers_up/deploy/operator/t${namespace_suffix}_teranode_v1alpha1_node.yaml -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground
}
```

### Main Execution Flow

1. **Check for Help Argument**: If `help` is passed as the first argument, display usage instructions and exit.

2. **Scale Up Operations**:
   - **If `all` is passed**: Scales up all regions (`t1`, `t2`, `t3`) concurrently.
   - **If `t1`, `t2`, or `t3` is passed**: Scales up the specified region and namespace.
   - **Unknown Arguments**: Prints an error message indicating that `t1`, `t2`, `t3` are sharing the same environment and prompts to specify one of them.

3. **Wait for Background Processes**: The script waits for all background processes to complete before exiting.

### Background Processes

- The script runs scale-up operations in parallel using background processes. Process IDs are collected and waited on to ensure completion.
