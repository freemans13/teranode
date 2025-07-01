#!/bin/bash

# Script to reset a Kubernetes namespace by:
# 1. Deleting all Aerospike PVCs
# 2. Deleting all Aerospike pods
# 3. Dropping all tables in PostgreSQL databases
# 4. Deleting all pods in the namespace

set -e

# Function to display usage information
usage() {
    echo "Usage: $0 [-n <namespace>] [--all-teranet-stage] [--all-teranet-prod]"
    echo ""
    echo "Options:"
    echo "  -n  Kubernetes namespace (required if not using --all-teranet-stage or --all-teranet-prod)"
    echo "      Format: stage-eks-X-teranet-Y, prod-eks-X-teranet-Y, stage-eks-X-mainnet-Y, or prod-eks-X-mainnet-Y"
    echo "  --all-teranet-stage  Reset all stage teranet instances (1, 2, and 3)"
    echo "  --all-teranet-prod   Reset all prod teranet instances (1, 2, and 3)"
    echo "  -y  Skip confirmation (use with caution)"
    echo "  -?  Display this help message"
    exit 1
}

# Function to get RDS endpoint based on environment
get_rds_endpoint() {
    local env_type="$1"
    local eks_num="$2"

    # Query AWS RDS to find the endpoint
    local db_identifier="${env_type}-eks-${eks_num}"

    echo "Looking up RDS endpoint for ${db_identifier}..." >&2

    local endpoint=$(aws rds describe-db-instances \
        --query "DBInstances[?contains(DBInstanceIdentifier, '${db_identifier}')].Endpoint.Address" \
        --output text 2>/dev/null)

    if [ -z "$endpoint" ] || [ "$endpoint" = "None" ]; then
        echo "Error: Could not find RDS instance matching '${db_identifier}'" >&2
        echo "Available RDS instances:" >&2
        aws rds describe-db-instances --query "DBInstances[].DBInstanceIdentifier" --output table >&2
        return 1
    fi

    # If multiple endpoints returned, take the first one
    endpoint=$(echo "$endpoint" | head -n1)

    echo "Found RDS endpoint: $endpoint" >&2
    echo "$endpoint"
}

# Default values
PG_PORT="5432"
SKIP_CONFIRM=false
ALL_TERANET_STAGE=false
ALL_TERANET_PROD=false
ALL_MAINNET_STAGE=false
ALL_MAINNET_PROD=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n)
            NAMESPACE="$2"
            if [[ $NAMESPACE == stage-* ]]; then
                PG_HOST="stage-eks-1.cvv3sjswqwrk.eu-central-1.rds.amazonaws.com"
            elif [[ $NAMESPACE == prod-* ]]; then
                PG_HOST="prod-eks-1.czklemh7vdzk.eu-west-1.rds.amazonaws.com"
            else
                echo "Error: Invalid namespace format. Must start with 'stage-' or 'prod-'"
                usage
            fi
            shift 2
            ;;
        --all-teranet-stage)
            ALL_TERANET_STAGE=true
            PG_HOST="stage-eks-1.cvv3sjswqwrk.eu-central-1.rds.amazonaws.com"

            shift
            ;;
        --all-teranet-prod)
            ALL_TERANET_PROD=true
            PG_HOST="prod-eks-1.czklemh7vdzk.eu-west-1.rds.amazonaws.com"
            shift
            ;;
        --all-mainnet-stage)
            ALL_MAINNET_STAGE=true
            PG_HOST="stage-eks-1.cvv3sjswqwrk.eu-central-1.rds.amazonaws.com"
            shift
            ;;
        --all-mainnet-prod)
            ALL_MAINNET_PROD=true
            PG_HOST="prod-eks-1.czklemh7vdzk.eu-west-1.rds.amazonaws.com"
            shift
            ;;
        -y)
            SKIP_CONFIRM=true
            shift
            ;;
        -\?|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Function to reset a single namespace
reset_namespace() {
    local NAMESPACE="$1"

    # Extract components from namespace
    if [[ "$NAMESPACE" =~ ^(stage|prod)-eks-([0-9]+)-(teranet|mainnet)-([0-9]+)$ ]]; then
        ENV_TYPE="${BASH_REMATCH[1]}"
        EKS_NUM="${BASH_REMATCH[2]}"
        NETWORK_TYPE="${BASH_REMATCH[3]}"
        INSTANCE_NUM="${BASH_REMATCH[4]}"
    else
        echo "Error: Namespace must be in format 'stage-eks-X-teranet-Y', 'prod-eks-X-teranet-Y', 'stage-eks-X-mainnet-Y', or 'prod-eks-X-mainnet-Y'"
        return 1
    fi

    # Get the RDS endpoint dynamically
    PG_HOST=$(get_rds_endpoint "$ENV_TYPE" "$EKS_NUM")
    if [ $? -ne 0 ]; then
        echo "Failed to get RDS endpoint for $ENV_TYPE-eks-$EKS_NUM"
        return 1
    fi

    # Derive other namespaces and database names
    AEROSPIKE_NS="aerospike-${ENV_TYPE}-eks-${NETWORK_TYPE}-$INSTANCE_NUM"
    APP_NS="$NAMESPACE"
    MAIN_DB="$NAMESPACE"
    COINBASE_DB="coinbase-$NAMESPACE"
    MAIN_DB_USER="$NAMESPACE"
    COINBASE_DB_USER="coinbase-$NAMESPACE"

    # Set the appropriate PostgreSQL host based on environment type
    if [[ "$ENV_TYPE" == "prod" ]]; then
        PG_HOST="prod-eks-1.czklemh7vdzk.eu-west-1.rds.amazonaws.com"
    else
        PG_HOST="stage-eks-1.cvv3sjswqwrk.eu-central-1.rds.amazonaws.com"
    fi

    # Confirm before proceeding
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This script will perform the following destructive operations:"
        echo "  - Delete all Aerospike PVCs in namespace '$AEROSPIKE_NS'"
        echo "  - Delete all Aerospike pods in namespace '$AEROSPIKE_NS'"
        echo "  - Delete cluster-storage PVC in namespace '$APP_NS'"
        echo "  - Drop ALL tables in PostgreSQL database:"
        echo "    * $MAIN_DB"
        if [[ "${NETWORK_TYPE}" == "teranet" ]]; then
            echo "    * $COINBASE_DB (only for teranet network type)"
        fi
        echo "  - Delete all pods in namespace '$APP_NS'"
        echo ""
        read -p "Are you sure you want to continue with namespace '$NAMESPACE'? (y/n): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled for namespace '$NAMESPACE'."
            return 1
        fi
    fi

    # Check if namespaces exist
    if ! kubectl get namespace "$AEROSPIKE_NS" &>/dev/null; then
        echo "Warning: Aerospike namespace '$AEROSPIKE_NS' does not exist"
    fi

    if ! kubectl get namespace "$APP_NS" &>/dev/null; then
        echo "Warning: Application namespace '$APP_NS' does not exist"
    fi

    echo "Starting reset process for namespace: $NAMESPACE"
    echo "Using PostgreSQL host: $PG_HOST"

    # Step 1: Delete all Aerospike PVCs in the namespace (in background)
    echo "Deleting Aerospike PVCs (in background)..."
    kubectl delete pvc -n "$AEROSPIKE_NS" --all &
    PVC_DELETE_PID=$!
    echo "Aerospike PVC deletion started in background (PID: $PVC_DELETE_PID)"

    # Step 2: Delete all Aerospike pods
    echo "Deleting Aerospike pods..."
    kubectl delete pod -n "$AEROSPIKE_NS" --all
    echo "Aerospike pods deleted."

    # Step 3: Connect to PostgreSQL and drop tables
    echo "Connecting to main PostgreSQL database '$MAIN_DB' on host '$PG_HOST' to drop tables..."
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$MAIN_DB_USER" -d "$MAIN_DB" -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;"
    echo "Tables dropped from main PostgreSQL database."

    echo "Connecting to coinbase PostgreSQL database '$COINBASE_DB' on host '$PG_HOST' to drop tables..."
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$COINBASE_DB_USER" -d "$COINBASE_DB" -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"
    echo "Tables dropped from coinbase PostgreSQL database."

    # Delete the cluster-storage PVC in the namespace in the background
    echo "Starting deletion of 'cluster-storage' PVC in namespace '$APP_NS' in the background..."
    kubectl delete pvc cluster-storage -n "$APP_NS" 2>/dev/null &
    CLUSTER_STORAGE_DELETE_PID=$!
    echo "PVC 'cluster-storage' deletion started in the background (PID: $CLUSTER_STORAGE_DELETE_PID)."

    # Step 4: Delete all pods in the namespace
    echo "Deleting all pods in namespace '$APP_NS'..."
    kubectl delete pods --all -n "$APP_NS" 2>/dev/null || echo "No pods found or unable to delete pods in namespace '$APP_NS'"
    echo "All pods deleted from namespace '$APP_NS'."

    # Check if PVC deletion is still running
    if kill -0 $PVC_DELETE_PID 2>/dev/null; then
        echo "Aerospike PVC deletion is still running in the background (PID: $PVC_DELETE_PID)"
        echo "You can check its status later with: kubectl get pvc -n $AEROSPIKE_NS"
    else
        echo "Aerospike PVC deletion completed."
    fi

    # Check if cluster-storage PVC deletion is still running
    if kill -0 $CLUSTER_STORAGE_DELETE_PID 2>/dev/null; then
        echo "Cluster-storage PVC deletion is still running in the background (PID: $CLUSTER_STORAGE_DELETE_PID)"
        echo "You can check its status later with: kubectl get pvc cluster-storage -n $APP_NS"
    else
        echo "Cluster-storage PVC deletion completed."
    fi

    echo "Reset process completed successfully for namespace: $NAMESPACE"
    echo "------------------------------------------------------------"
}

# Check if we're using one of the batch options
if [ "$ALL_TERANET_STAGE" = true ]; then
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This will reset ALL stage teranet instances (1, 2, and 3)"
        read -p "Are you sure you want to continue? (y/n): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled."
            exit 1
        fi
    fi

    for i in {1..3}; do
        reset_namespace "stage-eks-1-teranet-$i"
    done
    exit 0
elif [ "$ALL_TERANET_PROD" = true ]; then
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This will reset ALL production teranet instances (1, 2, and 3)"
        read -p "Are you sure you want to continue? (y/n): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled."
            exit 1
        fi
    fi

    for i in {1..3}; do
        reset_namespace "prod-eks-1-teranet-$i"
    done
    exit 0
elif [ "$ALL_MAINNET_STAGE" = true ]; then
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This will reset ALL stage mainnet instances (1, 2, and 3)"
        read -p "Are you sure you want to continue? (y/n): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled."
            exit 1
        fi
    fi

    reset_namespace "stage-eks-1-mainnet-1"
    exit 0
elif [ "$ALL_MAINNET_PROD" = true ]; then
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This will reset ALL production mainnet instances (1, 2, and 3)"
        read -p "Are you sure you want to continue? (y/n): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled."
            exit 1
        fi
    fi

    reset_namespace "prod-eks-1-mainnet-1"
    exit 0
elif [ -z "$NAMESPACE" ]; then
    echo "Error: You must specify either a namespace (-n) or use --all-teranet-stage or --all-teranet-prod"
    usage
else
    reset_namespace "$NAMESPACE"
fi
