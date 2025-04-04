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
    echo "      Format: stage-eks-X-teranet-Y or prod-eks-X-teranet-Y"
    echo "  --all-teranet-stage  Reset all stage teranet instances (1, 2, and 3)"
    echo "  --all-teranet-prod   Reset all prod teranet instances (1, 2, and 3)"
    echo "  -y  Skip confirmation (use with caution)"
    echo "  -?  Display this help message"
    exit 1
}

# Default values
PG_HOST="stage-eks-1.cvv3sjswqwrk.eu-central-1.rds.amazonaws.com"
PG_PORT="5432"
SKIP_CONFIRM=false
ALL_TERANET_STAGE=false
ALL_TERANET_PROD=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n)
            NAMESPACE="$2"
            shift 2
            ;;
        --all-teranet-stage)
            ALL_TERANET_STAGE=true
            shift
            ;;
        --all-teranet-prod)
            ALL_TERANET_PROD=true
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
    if [[ "$NAMESPACE" =~ ^(stage|prod)-eks-([0-9]+)-teranet-([0-9]+)$ ]]; then
        ENV_TYPE="${BASH_REMATCH[1]}"
        EKS_NUM="${BASH_REMATCH[2]}"
        TERANET_NUM="${BASH_REMATCH[3]}"
    else
        echo "Error: Namespace must be in format 'stage-eks-X-teranet-Y' or 'prod-eks-X-teranet-Y'"
        return 1
    fi

    # Derive other namespaces and database names
    AEROSPIKE_NS="aerospike-${ENV_TYPE}-eks-teranet-$TERANET_NUM"
    APP_NS="$NAMESPACE"
    MAIN_DB="$NAMESPACE"
    COINBASE_DB="coinbase-$NAMESPACE"
    MAIN_DB_USER="$NAMESPACE"
    COINBASE_DB_USER="coinbase-$NAMESPACE"

    # Confirm before proceeding
    if [ "$SKIP_CONFIRM" = false ]; then
        echo "WARNING: This script will perform the following destructive operations:"
        echo "  - Delete all Aerospike PVCs in namespace '$AEROSPIKE_NS'"
        echo "  - Delete all Aerospike pods in namespace '$AEROSPIKE_NS'"
        echo "  - Drop ALL tables in PostgreSQL databases:"
        echo "    * $MAIN_DB"
        echo "    * $COINBASE_DB"
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
    echo "Connecting to main PostgreSQL database '$MAIN_DB' to drop tables..."
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$MAIN_DB_USER" -d "$MAIN_DB" -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;"
    echo "Tables dropped from main PostgreSQL database."

    echo "Connecting to coinbase PostgreSQL database '$COINBASE_DB' to drop tables..."
    psql -h "$PG_HOST" -p "$PG_PORT" -U "$COINBASE_DB_USER" -d "$COINBASE_DB" -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"
    echo "Tables dropped from coinbase PostgreSQL database."

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
elif [ -z "$NAMESPACE" ]; then
    echo "Error: You must specify either a namespace (-n) or use --all-teranet-stage or --all-teranet-prod"
    usage
else
    reset_namespace "$NAMESPACE"
fi